//! Retain a single application writer between listener polls. Protocol writers
//! are released so health and operator status can recover their own snapshot.
//! A fresh protocol router must extend the exact prior application history.
use super::*;
use rsf_core::{CommitVersion, Digest};

#[derive(Debug)]
pub(super) struct ParkedApplication {
    namespace: String,
    application: Application,
    through: CommitVersion,
    history: Digest,
}

#[derive(Debug)]
enum Application {
    Kv(Box<DurableKvEngine>),
    Sql(Box<DurableSqlEngine>),
    Blob(Box<DurableBlobEngine>),
    Object(Box<DurableObjectStore>),
}

fn history(router: &DataRouter, through: CommitVersion) -> RsfResult<Digest> {
    let mut digest = Digest::blake3_bytes(b"RSF parked application history v1");
    for command in router.committed_application_commands() {
        if command.commit.version > through {
            continue;
        }
        let mut bytes = digest.as_bytes().to_vec();
        bytes.extend_from_slice(Digest::blake3_bytes(&command.encode()?).as_bytes());
        digest = Digest::blake3_bytes(&bytes);
    }
    Ok(digest)
}

impl DataPlaneExecutor {
    pub(super) fn park_application(&self) -> RsfResult<()> {
        let Some(mut cached) = self.cached.borrow_mut().take() else {
            return Ok(());
        };
        let router = cached.runtime.router();
        let through = router.latest_version();
        let history = history(router, through)?;
        let application = match cached.runtime {
            Runtime::Kv(runtime) => Application::Kv(Box::new(runtime.kv)),
            Runtime::Sql(runtime) => Application::Sql(Box::new(runtime.sql)),
            Runtime::Blob(runtime) => Application::Blob(Box::new(runtime.blobs)),
            Runtime::Object(runtime) => Application::Object(Box::new(runtime.objects)),
        };
        *self.parked.borrow_mut() = Some(ParkedApplication {
            namespace: cached.namespace,
            application,
            through,
            history,
        });
        Ok(())
    }
}

impl ParkedApplication {
    pub(super) fn matches(
        &self,
        namespace: &str,
        surface: rsf_protocol::ApplicationSurface,
    ) -> bool {
        let current = match self.application {
            Application::Kv(_) => rsf_protocol::ApplicationSurface::Kv,
            Application::Sql(_) => rsf_protocol::ApplicationSurface::Sql,
            Application::Blob(_) => rsf_protocol::ApplicationSurface::Blob,
            Application::Object(_) => rsf_protocol::ApplicationSurface::Object,
        };
        self.namespace == namespace && current == surface
    }

    pub(super) fn restore(self, mut router: DataRouter) -> RsfResult<Runtime> {
        if router.latest_version() < self.through || history(&router, self.through)? != self.history
        {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "recovered protocol history differs from the retained application prefix",
            ));
        }
        // No other application writer has held this store while parked. Only
        // commands beyond the exact retained prefix need materialization.
        let commands = router.committed_application_commands_since(self.through);
        Ok(match self.application {
            Application::Kv(mut kv) => {
                for command in commands {
                    if command.application.namespace == self.namespace
                        && command.application.surface == rsf_protocol::ApplicationSurface::Kv
                    {
                        kv.apply_application_command(&command)?;
                    }
                }
                kv.reconcile(&mut router)?;
                Runtime::Kv(Box::new(KvRuntime { router, kv: *kv }))
            }
            Application::Sql(mut sql) => {
                for command in commands {
                    if command.application.namespace == self.namespace
                        && command.application.surface == rsf_protocol::ApplicationSurface::Sql
                    {
                        sql.apply_application_command(&command)?;
                    }
                }
                sql.reconcile(&mut router)?;
                Runtime::Sql(Box::new(SqlRuntime { router, sql: *sql }))
            }
            Application::Blob(mut blobs) => {
                for command in commands {
                    if command.application.namespace == self.namespace
                        && command.application.surface == rsf_protocol::ApplicationSurface::Blob
                    {
                        blobs.apply_application_command(&command)?;
                    }
                }
                Runtime::Blob(Box::new(BlobRuntime {
                    router,
                    blobs: *blobs,
                }))
            }
            Application::Object(mut objects) => {
                for command in commands {
                    if command.application.namespace == self.namespace
                        && command.application.surface == rsf_protocol::ApplicationSurface::Object
                    {
                        objects.apply_application_command(&command)?;
                    }
                }
                objects.reconcile(&mut router)?;
                Runtime::Object(Box::new(ObjectRuntime {
                    router,
                    objects: *objects,
                }))
            }
        })
    }
}

#[cfg(test)]
mod tests;

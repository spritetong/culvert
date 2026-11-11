// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! TCP / Unix domain socket frontend. Wraps the OS-level `GenericListener` so
//! no core or tunnel module names a concrete listener type.
//! All comments and docstrings must be in English.

use crate::frontend::{Frontend, FrontendAccept, FrontendFactory, FrontendOutput};
use crate::io::addr::{L4Protocol, MappingSpec};
use crate::io::channel::IoChannel;
use crate::io::error::TunnelError;
use crate::io::stream::BoxFuture;
use crate::platform::GenericListener;
use reusable_future::ReusableFutureRef;

/// The only registered frontend factory today.
pub struct TcpFrontendFactory;

impl FrontendFactory for TcpFrontendFactory {
    fn protocol(&self) -> L4Protocol {
        L4Protocol::Tcp
    }

    fn bind(
        &self,
        spec: MappingSpec,
    ) -> BoxFuture<'static, Result<Box<dyn Frontend>, TunnelError>> {
        Box::pin(async move {
            let listener = GenericListener::bind(&spec.listen).await?;
            Ok(Box::new(TcpFrontend { listener }) as Box<dyn Frontend>)
        })
    }
}

pub struct TcpFrontend {
    listener: GenericListener,
}

impl Frontend for TcpFrontend {
    fn accept(&mut self, slot: &mut ReusableFutureRef<'static, FrontendOutput>) {
        if !slot.is_empty() {
            return;
        }
        let listener = self.listener.clone();
        slot.set(move || async move {
            let (stream, peer) = listener.accept_owned().await?;
            Ok(FrontendAccept {
                channel: IoChannel::from_boxed_stream(stream),
                peer,
            })
        });
    }

    fn shutdown<'a>(&'a mut self) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}

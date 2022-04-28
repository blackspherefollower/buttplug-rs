// Buttplug Rust Source Code File - See https://buttplug.io for more info.
//
// Copyright 2016-2019 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

//! Handles client sessions, as well as discovery and communication with hardware.

pub mod comm_managers;
pub mod device_manager;
mod device_manager_event_loop;
mod ping_timer;
pub mod remote_server;

pub use remote_server::ButtplugRemoteServer;

use crate::{
  core::{
    errors::*,
    messages::{
      self,
      ButtplugClientMessage,
      ButtplugDeviceCommandMessageUnion,
      ButtplugDeviceManagerMessageUnion,
      ButtplugMessage,
      ButtplugServerMessage,
      StopAllDevices,
      StopScanning,
      BUTTPLUG_CURRENT_MESSAGE_SPEC_VERSION,
    },
  },
  util::{
    async_manager,
    device_configuration::{load_protocol_config_from_json, DEVICE_CONFIGURATION_JSON},
    stream::convert_broadcast_receiver_to_stream,
  },
};
use device_manager::DeviceManager;
use futures::{
  future::{self, BoxFuture},
  Stream,
};
use ping_timer::PingTimer;
use std::{
  fmt,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc
  },
};
use thiserror::Error;
use tokio::sync::broadcast;
use tracing_futures::Instrument;

pub type ButtplugServerResult = Result<ButtplugServerMessage, ButtplugError>;
pub type ButtplugServerResultFuture = BoxFuture<'static, ButtplugServerResult>;

#[derive(Error, Debug)]
pub enum ButtplugServerError {
  #[error("DeviceManager of type {0} has already been added.")]
  DeviceManagerTypeAlreadyAdded(String),
  #[error("Buttplug Protocol of type {0} has already been added to the system.")]
  ProtocolAlreadyAdded(String),
  #[error("Buttplug Protocol of type {0} does not exist in the system and cannot be removed.")]
  ProtocolDoesNotExist(String),
}

#[derive(Debug, Clone)]
pub struct ButtplugServerBuilder {
  pub name: String,
  pub max_ping_time: Option<u32>,
  pub allow_raw_messages: bool,
  pub device_configuration_json: Option<String>,
  pub user_device_configuration_json: Option<String>,
}

impl Default for ButtplugServerBuilder {
  fn default() -> Self {
    Self {
      name: "Buttplug Server".to_owned(),
      max_ping_time: None,
      allow_raw_messages: false,
      device_configuration_json: Some(DEVICE_CONFIGURATION_JSON.to_owned()),
      user_device_configuration_json: None,
    }
  }
}

impl ButtplugServerBuilder {
  pub fn name(&mut self, name: &str) -> &mut Self {
    self.name = name.to_owned();
    self
  }

  pub fn max_ping_time(&mut self, ping_time: u32) -> &mut Self {
    self.max_ping_time = Some(ping_time);
    self
  }

  pub fn allow_raw_messages(&mut self, allow: bool) -> &mut Self {
    self.allow_raw_messages = allow;
    self
  }

  pub fn device_configuration_json(&mut self, config_json: Option<String>) -> &mut Self {
    self.device_configuration_json = config_json;
    self
  }

  pub fn user_device_configuration_json(&mut self, config_json: Option<String>) -> &mut Self {
    self.user_device_configuration_json = config_json;
    self
  }

  pub fn finish(&self) -> Result<ButtplugServer, ButtplugError> {
    // If the user config string exists, parse it.
    let user_config = if let Some(user_device_config) = &self.user_device_configuration_json {
      // Skip checking the version of user device config files for now.
      Some(load_protocol_config_from_json(user_device_config, true)?)
    } else {
      None
    };

    // If the device config string exists, parse it.
    let device_config = if let Some(main_device_config) = &self.device_configuration_json {
      let mut main_config = load_protocol_config_from_json(main_device_config, false)?;
      if let Some(user_config) = user_config {
        main_config.merge(user_config);
      }
      Some(main_config)
    } else {
      user_config
    };

    // Create the server
    debug!("Creating server '{}'", self.name);
    info!("Buttplug Server Operating System Info: {}", os_info::get());
    let (send, _) = broadcast::channel(256);
    let output_sender_clone = send.clone();
    let connected = Arc::new(AtomicBool::new(false));
    let ping_time = self.max_ping_time.unwrap_or(0);
    let ping_timer = Arc::new(PingTimer::new(ping_time));
    let ping_timeout_notifier = ping_timer.ping_timeout_waiter();
    let connected_clone = connected.clone();
    async_manager::spawn(
      async move {
        // This will only exit if we've pinged out.
        ping_timeout_notifier.await;
        error!("Ping out signal received, stopping server");
        connected_clone.store(false, Ordering::SeqCst);
        // TODO Should the event sender return a result instead of an error message?
        if output_sender_clone
          .send(messages::Error::from(ButtplugError::from(ButtplugPingError::PingedOut)).into())
          .is_err()
        {
          error!("Server disappeared, cannot update about ping out.");
        };
      }
      .instrument(tracing::info_span!("Buttplug Server Ping Timeout Task")),
    );
    let device_manager =
      DeviceManager::new(send.clone(), ping_timer.clone(), self.allow_raw_messages);

    if let Some(devices) = device_config {
      for (name, def) in devices.protocols {
        device_manager.add_protocol_definition(&name, def);
      }
      for (address, user_config) in devices.user_config {
        device_manager.add_device_user_config(&address, user_config);
      }
    }

    let server = ButtplugServer {
      server_name: self.name.clone(),
      max_ping_time: ping_time,
      device_manager,
      ping_timer,
      connected,
      output_sender: send,
    };

    // Add the device config

    // Update with the user config

    // Assuming everything passed, return the server.
    Ok(server)
  }
}

/// Represents a ButtplugServer.
pub struct ButtplugServer {
  server_name: String,
  max_ping_time: u32,
  device_manager: DeviceManager,
  ping_timer: Arc<PingTimer>,
  connected: Arc<AtomicBool>,
  output_sender: broadcast::Sender<ButtplugServerMessage>,
}

impl std::fmt::Debug for ButtplugServer {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ButtplugServer")
      .field("server_name", &self.server_name)
      .field("max_ping_time", &self.max_ping_time)
      .field("connected", &self.connected)
      .finish()
  }
}

impl Default for ButtplugServer {
  fn default() -> Self {
    // We can unwrap here because if default init fails, so will pretty much every test.
    ButtplugServerBuilder::default()
      .finish()
      .expect("Default is infallible")
  }
}

impl ButtplugServer {
  pub fn event_stream(&self) -> impl Stream<Item = ButtplugServerMessage> {
    // Unlike the client API, we can expect anyone using the server to pin this
    // themselves.
    convert_broadcast_receiver_to_stream(self.output_sender.subscribe())
  }

  pub fn device_manager(&self) -> &DeviceManager {
    &self.device_manager
  }

  pub fn connected(&self) -> bool {
    self.connected.load(Ordering::SeqCst)
  }

  pub fn disconnect(&self) -> BoxFuture<Result<(), messages::Error>> {
    debug!("Buttplug Server {} disconnect requested", self.server_name);
    let ping_timer = self.ping_timer.clone();
    let stop_scanning_fut =
      self.parse_message(ButtplugClientMessage::StopScanning(StopScanning::default()));
    let stop_fut = self.parse_message(ButtplugClientMessage::StopAllDevices(
      StopAllDevices::default(),
    ));
    let connected = self.connected.clone();
    Box::pin(async move {
      connected.store(false, Ordering::SeqCst);
      ping_timer.stop_ping_timer().await;
      // Ignore returns here, we just want to stop.
      info!("Server disconnected, stopping device scanning if it was started...");
      let _ = stop_scanning_fut.await;
      info!("Server disconnected, stopping all devices...");
      let _ = stop_fut.await;
      Ok(())
    })
  }

  // This is the only method that returns ButtplugServerResult, as it handles
  // the packing of the message ID.
  pub fn parse_message(
    &self,
    msg: ButtplugClientMessage,
  ) -> BoxFuture<'static, Result<ButtplugServerMessage, messages::Error>> {
    trace!(
      "Buttplug Server {} received message to client parse: {:?}",
      self.server_name,
      msg
    );
    let id = msg.id();
    if !self.connected() {
      // Check for ping timeout first! There's no way we should've pinged out if
      // we haven't received RequestServerInfo first, but we do want to know if
      // we pinged out.
      let error = if self.ping_timer.pinged_out() {
        Some(messages::Error::from(ButtplugError::from(
          ButtplugPingError::PingedOut,
        )))
      } else if !matches!(msg, ButtplugClientMessage::RequestServerInfo(_)) {
        Some(messages::Error::from(ButtplugError::from(
          ButtplugHandshakeError::RequestServerInfoExpected,
        )))
      } else {
        None
      };
      if let Some(mut return_error) = error {
        return_error.set_id(msg.id());
        return Box::pin(future::ready(Err(return_error)));
      }
      // If we haven't pinged out and we got an RSI message, fall thru.
    }
    // Produce whatever future is needed to reply to the message, this may be a
    // device command future, or something the server handles. All futures will
    // return Result<ButtplugServerMessage, ButtplugError>, and we'll handle
    // tagging the result with the message id in the future we put out as the
    // return value from this method.
    let out_fut = if ButtplugDeviceManagerMessageUnion::try_from(msg.clone()).is_ok()
      || ButtplugDeviceCommandMessageUnion::try_from(msg.clone()).is_ok()
    {
      self.device_manager.parse_message(msg.clone())
    } else {
      match msg {
        ButtplugClientMessage::RequestServerInfo(rsi_msg) => self.perform_handshake(rsi_msg),
        ButtplugClientMessage::Ping(p) => self.handle_ping(p),
        _ => ButtplugMessageError::UnexpectedMessageType(format!("{:?}", msg)).into(),
      }
    };
    // Simple way to set the ID on the way out. Just rewrap
    // the returned future to make sure it happens.
    Box::pin(
      async move {
        out_fut
          .await
          .map(|mut ok_msg| {
            ok_msg.set_id(id);
            ok_msg
          })
          .map_err(|err| {
            let mut error = messages::Error::from(err);
            error.set_id(id);
            error
          })
      }
      .instrument(info_span!("Buttplug Server Message", id = id)),
    )
  }

  fn perform_handshake(&self, msg: messages::RequestServerInfo) -> ButtplugServerResultFuture {
    if self.connected() {
      return ButtplugHandshakeError::HandshakeAlreadyHappened.into();
    }
    info!(
      "Performing server handshake check with client {} at message version {}.",
      msg.client_name(),
      msg.message_version()
    );
    if BUTTPLUG_CURRENT_MESSAGE_SPEC_VERSION < msg.message_version() {
      return ButtplugHandshakeError::MessageSpecVersionMismatch(
        BUTTPLUG_CURRENT_MESSAGE_SPEC_VERSION,
        msg.message_version(),
      )
      .into();
    }
    // Only start the ping timer after we've received the handshake.
    let ping_timer = self.ping_timer.clone();
    let out_msg = messages::ServerInfo::new(
      &self.server_name,
      BUTTPLUG_CURRENT_MESSAGE_SPEC_VERSION,
      self.max_ping_time,
    );
    let connected = self.connected.clone();
    Box::pin(async move {
      ping_timer.start_ping_timer().await;
      connected.store(true, Ordering::SeqCst);
      debug!("Server handshake check successful.");
      Result::Ok(out_msg.into())
    })
  }

  fn handle_ping(&self, msg: messages::Ping) -> ButtplugServerResultFuture {
    if self.max_ping_time == 0 {
      return ButtplugPingError::PingTimerNotRunning.into();
    }
    let fut = self.ping_timer.update_ping_time();
    Box::pin(async move {
      fut.await;
      Result::Ok(messages::Ok::new(msg.id()).into())
    })
  }
}

#[cfg(test)]
mod test {
  use crate::{
    core::messages::{self, BUTTPLUG_CURRENT_MESSAGE_SPEC_VERSION},
    server::ButtplugServer,
    util::async_manager,
  };

  #[test]
  fn test_server_reuse() {
    async_manager::block_on(async {
      let server = ButtplugServer::default();
      let msg =
        messages::RequestServerInfo::new("Test Client", BUTTPLUG_CURRENT_MESSAGE_SPEC_VERSION);
      let mut reply = server.parse_message(msg.clone().into()).await;
      assert!(reply.is_ok(), "Should get back ok: {:?}", reply);

      reply = server.parse_message(msg.clone().into()).await;
      assert!(
        reply.is_err(),
        "Should get back err on double handshake: {:?}",
        reply
      );
      assert!(server.disconnect().await.is_ok(), "Should disconnect ok");

      reply = server.parse_message(msg.clone().into()).await;
      assert!(
        reply.is_ok(),
        "Should get back ok on handshake after disconnect: {:?}",
        reply
      );
    });
  }
}

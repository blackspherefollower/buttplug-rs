// Buttplug Rust Source Code File - See https://buttplug.io for more info.
//
// Copyright 2016-2025 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

use super::fleshlight_launch_helper;
use crate::server::device::hardware::{HardwareEvent, HardwareSubscribeCmd};
use crate::server::device::protocol::thehandy_v4::handy_rpc::{rpc_message, RpcMessage};
use crate::{
  core::{
    errors::ButtplugDeviceError,
    message::{self, ButtplugDeviceMessage, Endpoint},
  },
  server::device::{
    configuration::{ProtocolCommunicationSpecifier, UserDeviceDefinition, UserDeviceIdentifier},
    hardware::{Hardware, HardwareCommand, HardwareWriteCmd},
    protocol::{
      generic_protocol_initializer_setup,
      ProtocolHandler,
      ProtocolIdentifier,
      ProtocolInitializer,
    },
  },
};
use async_trait::async_trait;
use prost::Message;
use std::sync::{
  atomic::{AtomicU8, Ordering},
  Arc,
};
use std::time::Duration;
use tokio::time::sleep;

mod handy_rpc {
  include!(concat!(env!("OUT_DIR"), "/hdy_rpc.rs"));
}

generic_protocol_initializer_setup!(TheHandyV4, "thehandy-v4");

#[derive(Default)]
pub struct TheHandyV4Initializer {}

#[async_trait]
impl ProtocolInitializer for TheHandyV4Initializer {
  async fn initialize(
    &mut self,
    hardware: Arc<Hardware>,
    _: &UserDeviceDefinition,
  ) -> Result<Arc<dyn ProtocolHandler>, ButtplugDeviceError> {
    let mut event_receiver = hardware.event_stream();
    let mut count = 0;
    hardware
      .subscribe(&HardwareSubscribeCmd::new(Endpoint::Rx))
      .await?;

    let ping_payload = handy_rpc::RpcMessage {
      r#type: handy_rpc::MessageType::Request as i32,
      message: Some(handy_rpc::rpc_message::Message::Request(
        handy_rpc::Request {
          id: 1,
          params: Some(handy_rpc::request::Params::RequestBatteryGet(
            handy_rpc::RequestBatteryGet {},
          )),
        },
      )),
    };
    let mut ping_buf = vec![];
    ping_payload
      .encode(&mut ping_buf)
      .expect("Infallible encode.");

    debug!("Send {:02X?}", ping_buf);
    let msg = HardwareWriteCmd::new(Endpoint::Tx, ping_buf, false);
    hardware.write_value(&msg).await?;

    let event = event_receiver.recv().await;
    if let Ok(HardwareEvent::Notification(_, _, n)) = event {
      debug!(
        "Got {:02X?} {:?}",
        n,
        handy_rpc::RpcMessage::decode(n.as_slice()) // We get battery info!
      );
    }

    let ping_payload = handy_rpc::RpcMessage {
      r#type: handy_rpc::MessageType::Request as i32,
      message: Some(handy_rpc::rpc_message::Message::Request(
        handy_rpc::Request {
          id: 2,
          params: Some(handy_rpc::request::Params::RequestCapabilitiesGet(
            handy_rpc::RequestCapabilitiesGet {},
          )),
        },
      )),
    };
    let mut ping_buf = vec![];
    ping_payload
      .encode(&mut ping_buf)
      .expect("Infallible encode.");

    debug!("Send {:02X?}", ping_buf);
    let msg = HardwareWriteCmd::new(Endpoint::Tx, ping_buf, false);
    hardware.write_value(&msg).await?;

    let event = event_receiver.recv().await;
    if let Ok(HardwareEvent::Notification(_, _, n)) = event {
      debug!(
        "Got {:02X?} {:?}",
        n,
        handy_rpc::RpcMessage::decode(n.as_slice())
      );
      // I'm only getting errors back for this... Which suggests the protobuf is out-of-sync for my firmware?
    }

    Ok(Arc::new(TheHandyV4::default()))
  }
}

#[derive(Default)]
pub struct TheHandyV4 {}

impl ProtocolHandler for TheHandyV4 {
  fn keepalive_strategy(&self) -> super::ProtocolKeepaliveStrategy {
    let ping_payload = handy_rpc::Request {
      id: 999,
      params: Some(handy_rpc::request::Params::RequestCapabilitiesGet(
        handy_rpc::RequestCapabilitiesGet {},
      )),
    };
    let mut ping_buf = vec![];
    ping_payload
      .encode(&mut ping_buf)
      .expect("Infallible encode.");

    super::ProtocolKeepaliveStrategy::RepeatPacketStrategy(HardwareWriteCmd::new(
      Endpoint::Tx,
      ping_buf,
      true,
    ))
  }

  fn handle_linear_cmd(
    &self,
    message: message::LinearCmdV4,
  ) -> Result<Vec<HardwareCommand>, ButtplugDeviceError> {
    if message.vectors().len() != 1 {
      return Err(ButtplugDeviceError::DeviceFeatureCountMismatch(
        1,
        message.vectors().len() as u32,
      ));
    }

    // This one's just a guess
    let linear_payload = handy_rpc::RpcMessage {
      r#type: handy_rpc::MessageType::Request as i32,
      message: Some(handy_rpc::rpc_message::Message::Request(
        handy_rpc::Request {
          id: 2, //Yay cargo cult
          params: Some(handy_rpc::request::Params::RequestHdspXpTSet(
            handy_rpc::RequestHdspXpTSet {
              stop_on_target: true,
              t: message.vectors()[0].duration(),
              xp: message.vectors()[0].position() as f32,
            },
          )),
        },
      )),
    };
    let mut linear_buf = vec![];
    linear_payload
      .encode(&mut linear_buf)
      .expect("Infallible encode.");
    Ok(vec![
      HardwareWriteCmd::new(Endpoint::Tx, linear_buf, true).into()
    ])
  }

  fn handle_scalar_vibrate_cmd(
    &self,
    _index: u32,
    scalar: u32,
  ) -> Result<Vec<HardwareCommand>, ButtplugDeviceError> {
    let payload = handy_rpc::RpcMessage {
      r#type: handy_rpc::MessageType::Request as i32,
      message: Some(handy_rpc::rpc_message::Message::Request(
        handy_rpc::Request {
          id: 2, //Yay cargo cult
          params: Some(if scalar == 0 {
            handy_rpc::request::Params::RequestHvpStop(handy_rpc::RequestHvpStop {})
          } else {
            handy_rpc::request::Params::RequestHvpSet(handy_rpc::RequestHvpSet {
              position: 0.0,                     // does nothing?
              amplitude: scalar as f32 / 100f32, // vibe speed
              frequency: 100,                    // Not sure how to map this...
            })
          }),
        },
      )),
    };
    let mut buf = vec![];
    payload.encode(&mut buf).expect("Infallible encode.");
    Ok(vec![HardwareWriteCmd::new(Endpoint::Tx, buf, true).into()])
  }
}

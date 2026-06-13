// Buttplug Rust Source Code File - See https://buttplug.io for more info.
//
// Copyright 2016-2026 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

use crate::device::{
  hardware::{Hardware, HardwareCommand, HardwareReadCmd, HardwareWriteCmd},
  protocol::{ProtocolHandler, ProtocolIdentifier, ProtocolInitializer, ProtocolKeepaliveStrategy},
};
use async_trait::async_trait;
use buttplug_core::errors::ButtplugDeviceError;
use buttplug_server_device_config::Endpoint;
use buttplug_server_device_config::{
  ProtocolCommunicationSpecifier,
  ServerDeviceDefinition,
  UserDeviceIdentifier,
};
use std::{
  sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
  },
  time::Duration,
};
use std::sync::atomic::{AtomicU16, AtomicU32};
use uuid::{Uuid, uuid};

const UMOVE_PROTOCOL_UUID: Uuid = uuid!("64afeb97-26ed-4c8e-b67a-5ae43dd2865d");

pub mod setup {
  use crate::device::protocol::{ProtocolIdentifier, ProtocolIdentifierFactory};
  #[derive(Default)]
  pub struct UmoveIdentifierFactory {}

  impl ProtocolIdentifierFactory for UmoveIdentifierFactory {
    fn identifier(&self) -> &str {
      "umove"
    }

    fn create(&self) -> Box<dyn ProtocolIdentifier> {
      Box::new(super::UmoveIdentifier::default())
    }
  }
}

#[derive(Default)]
pub struct UmoveIdentifier {}

#[async_trait]
impl ProtocolIdentifier for UmoveIdentifier {
  async fn identify(
    &mut self,
    hardware: Arc<Hardware>,
    specifier: ProtocolCommunicationSpecifier,
  ) -> Result<(UserDeviceIdentifier, Box<dyn ProtocolInitializer>), ButtplugDeviceError> {
    let device_identifier = hardware.name()[2..5].to_string();
    Ok((
      UserDeviceIdentifier::new(hardware.address(), "umove", &Some(device_identifier)),
      Box::new(UmoveInitializer::default()),
    ))
  }
}

#[derive(Default)]
pub struct UmoveInitializer {}

#[async_trait]
impl ProtocolInitializer for UmoveInitializer {
  async fn initialize(
    &mut self,
    hardware: Arc<Hardware>,
    device_definition: &ServerDeviceDefinition,
  ) -> Result<Arc<dyn ProtocolHandler>, ButtplugDeviceError> {
    Ok(Arc::new(Umove::default()))
  }
}

#[derive(Default)]
pub struct Umove {
  vibrate: Arc<AtomicU16>,
  move_target: Arc<AtomicU32>,
  move_time: Arc<AtomicU32>,
}

fn form_command(vibe: u16, pos: u32, time: u32) -> Vec<u8> {
  let mut data = vec![0x5A, 0xA5, 0x55, 0x00];

  data.append(&mut vibe.to_le_bytes().to_vec());
  data.append(&mut 1u32.to_le_bytes().to_vec());
  data.append(&mut time.to_le_bytes().to_vec());
  data.append(&mut pos.to_le_bytes().to_vec());
  data

}

impl ProtocolHandler for Umove {
  fn keepalive_strategy(&self) -> ProtocolKeepaliveStrategy {
    ProtocolKeepaliveStrategy::RepeatLastPacketStrategyWithTiming(Duration::from_millis(500))
  }

  fn handle_output_vibrate_cmd(
    &self,
    feature_index: u32,
    _feature_id: Uuid,
    speed: u32,
  ) -> Result<Vec<HardwareCommand>, ButtplugDeviceError> {
    Ok(vec![
      HardwareWriteCmd::new(&[UMOVE_PROTOCOL_UUID], Endpoint::Tx, vec![0x00], false).into(),
    ])
  }
}

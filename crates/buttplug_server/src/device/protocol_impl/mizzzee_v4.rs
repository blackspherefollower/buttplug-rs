// Buttplug Rust Source Code File - See https://buttplug.io for more info.
//
// Copyright 2016-2026 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

use crate::device::{
  hardware::{HardwareCommand, HardwareWriteCmd},
  protocol::{ProtocolHandler, ProtocolKeepaliveStrategy, generic_protocol_setup},
};
use buttplug_core::errors::ButtplugDeviceError;
use buttplug_server_device_config::Endpoint;
use std::time::Duration;
use uuid::Uuid;

generic_protocol_setup!(MizzZeeV4, "mizzzee-v4");

// Time between MizzZee v4 update commands, in milliseconds.
const MIZZZEE4_COMMAND_DELAY_MS: u64 = 300;

#[derive(Default)]
pub struct MizzZeeV4 {}

impl ProtocolHandler for MizzZeeV4 {
  fn keepalive_strategy(&self) -> ProtocolKeepaliveStrategy {
    ProtocolKeepaliveStrategy::RepeatLastPacketStrategyWithTiming(Duration::from_millis(
      MIZZZEE4_COMMAND_DELAY_MS,
    ))
  }

  fn handle_output_vibrate_cmd(
    &self,
    _feature_index: u32,
    feature_id: Uuid,
    speed: u32,
  ) -> Result<Vec<HardwareCommand>, ButtplugDeviceError> {
    Ok(vec![
      HardwareWriteCmd::new(
        &[feature_id],
        Endpoint::Tx,
        if speed == 0 {
          vec![0xA1, 0x0B, 0xAC]
        } else {
          vec![0xA2, speed as u8, 0xA2 + speed as u8]
        },
        true,
      )
      .into(),
    ])
  }
}

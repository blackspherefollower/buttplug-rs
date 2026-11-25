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

generic_protocol_setup!(MizzZeeV5, "mizzzee-v5");

// Time between MizzZee v5 update commands, in milliseconds.
const MIZZZEE5_COMMAND_DELAY_MS: u64 = 200;

#[derive(Default)]
pub struct MizzZeeV5 {}

impl ProtocolHandler for MizzZeeV5 {
  fn keepalive_strategy(&self) -> ProtocolKeepaliveStrategy {
    ProtocolKeepaliveStrategy::RepeatLastPacketStrategyWithTiming(Duration::from_millis(
      MIZZZEE5_COMMAND_DELAY_MS,
    ))
  }

  fn handle_output_vibrate_cmd(
    &self,
    _feature_index: u32,
    feature_id: Uuid,
    speed: u32,
  ) -> Result<Vec<HardwareCommand>, ButtplugDeviceError> {
    let mut data = vec![
      0xAAu8,
      0x08,
      0x02,
      speed as u8,
      if speed == 0 { 0 } else { 100 },
    ];
    let checksum = data.iter().fold(0u8, |sum, byte| sum.wrapping_add(*byte));
    data.push(checksum);

    Ok(vec![
      HardwareWriteCmd::new(&[feature_id], Endpoint::Tx, data, true).into(),
    ])
  }
}

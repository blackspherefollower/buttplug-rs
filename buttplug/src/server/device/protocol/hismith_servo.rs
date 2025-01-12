// Buttplug Rust Source Code File - See https://buttplug.io for more info.
//
// Copyright 2016-2024 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

use crate::{
  core::{
    errors::ButtplugDeviceError,
    message::{self, ActuatorType, ButtplugDeviceMessage, Endpoint, FeatureType, SensorReadingV4},
  },
  server::device::{
    configuration::{ProtocolCommunicationSpecifier, UserDeviceDefinition, UserDeviceIdentifier},
    hardware::{Hardware, HardwareCommand, HardwareEvent, HardwareSubscribeCmd, HardwareWriteCmd},
    protocol::{ProtocolHandler, ProtocolIdentifier, ProtocolInitializer},
  },
  util::{async_manager, sleep},
};
use async_trait::async_trait;
use futures::{future::BoxFuture, FutureExt};
use regex::Regex;
use std::{
  sync::{
    atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering},
    Arc,
  },
  time::Duration,
};
use crate::server::device::hardware;
use crate::server::device::hardware::HardwareReadCmd;

pub mod setup {
  use crate::server::device::protocol::{ProtocolIdentifier, ProtocolIdentifierFactory};
  #[derive(Default)]
  pub struct HismithServoIdentifierFactory {}

  impl ProtocolIdentifierFactory for HismithServoIdentifierFactory {
    fn identifier(&self) -> &str {
      "hismith-servo"
    }

    fn create(&self) -> Box<dyn ProtocolIdentifier> {
      Box::new(super::HismithServoIdentifier::default())
    }
  }
}

#[derive(Default)]
pub struct HismithServoIdentifier {}

#[async_trait]
impl ProtocolIdentifier for HismithServoIdentifier {
  async fn identify(
    &mut self,
    hardware: Arc<Hardware>,
    _: ProtocolCommunicationSpecifier,
  ) -> Result<(UserDeviceIdentifier, Box<dyn ProtocolInitializer>), ButtplugDeviceError> {
    let result = hardware
        .read_value(&HardwareReadCmd::new(Endpoint::RxBLEModel, 128, 500))
        .await?;

    let identifier = result
        .data()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    info!("Hismith Device Identifier: {}", identifier);

    Ok((
      UserDeviceIdentifier::new(hardware.address(), "hismith-servo", &Some(identifier)),
      Box::new(HismithServoInitializer::default()),
    ))
  }
}

#[derive(Default)]
pub struct HismithServoInitializer {}

#[async_trait]
impl ProtocolInitializer for HismithServoInitializer {
  async fn initialize(
    &mut self,
    hardware: Arc<Hardware>,
    _device_definition: &UserDeviceDefinition,
  ) -> Result<Arc<dyn ProtocolHandler>, ButtplugDeviceError> {
    hardware.write_value(&HardwareWriteCmd::new(Endpoint::Tx, vec![0xcc, 0x01, 0xa1, 0xa2], true)).await?;
    hardware.write_value(&HardwareWriteCmd::new(Endpoint::Tx, vec![0xcc, 0x09, 0x0a, 0x13], true)).await?;
    Ok(Arc::new(HismithServo::new(hardware)))
  }
}

pub struct HismithServo {
linear_info: Arc < (AtomicU8, AtomicU32) >,
}

impl HismithServo {
  pub fn new(
    hardware: Arc<Hardware>,) -> Self {
      let linear_info = Arc::new((AtomicU8::new(0), AtomicU32::new(0)));
      async_manager::spawn(update_linear_movement(
      hardware.clone(),
      linear_info.clone(),
      ));
      Self {
      linear_info,
    }
  }
}

impl ProtocolHandler for HismithServo {
  fn keepalive_strategy(&self) -> super::ProtocolKeepaliveStrategy {
    super::ProtocolKeepaliveStrategy::RepeatLastPacketStrategy
  }

  fn handle_linear_cmd(
    &self,
    message: message::LinearCmdV4,
  ) -> Result<Vec<HardwareCommand>, ButtplugDeviceError> {
    let vector = message
      .vectors()
      .first()
      .expect("Already checked for vector subcommand");
    self
      .linear_info
      .0
      .store((vector.position() * 100f64) as u8, Ordering::SeqCst);
    self
      .linear_info
      .1
      .store(vector.duration(), Ordering::SeqCst);
    info!("stored {} {}", (vector.position() * 100f64) as u8, vector.duration());
    Ok(vec![])
  }
}

async fn update_linear_movement(device: Arc<Hardware>, linear_info: Arc<(AtomicU8, AtomicU32)>) {
  let mut last_goal_position = 0i32;
  let mut current_move_amount = 0i32;
  let mut current_position = 0i32;
  loop {
    // See if we've updated our goal position
    let goal_position = linear_info.0.load(Ordering::Relaxed) as i32;
    // If we have and it's not the same, recalculate based on current status.
    if last_goal_position != goal_position {
      last_goal_position = goal_position;
      // We move every 100ms, so divide the movement into that many chunks.
      // If we're moving so fast it'd be under our 100ms boundary, just move in 1 step.
      let move_steps = (linear_info.1.load(Ordering::Relaxed) / 100).max(1);
      current_move_amount = (goal_position as i32 - current_position) as i32 / move_steps as i32;
    }
    info!("looping {} vs {} in {}ms", goal_position, current_position, linear_info.1.load(Ordering::SeqCst));

    // If we aren't going anywhere, just pause then restart
    if current_position == last_goal_position {
      sleep(Duration::from_millis(100)).await;
      continue;
    }

    // Update our position, make sure we don't overshoot
    current_position += current_move_amount;
    if current_move_amount < 0 {
      if current_position < last_goal_position {
        current_position = last_goal_position;
      }
    } else {
      if current_position > last_goal_position {
        current_position = last_goal_position;
      }
    }

    info!("move {:?}", vec![0xcc, 0x0a, current_position as u8, current_position as u8 + 0x0a]);
    let hardware_cmd = HardwareWriteCmd::new(Endpoint::Tx, vec![0xcc, 0x0a, current_position as u8, current_position as u8 + 0x0a], false);
    if device.write_value(&hardware_cmd).await.is_err() {
      return;
    }
    sleep(Duration::from_millis(100)).await;
  }
}

// Buttplug Rust Source Code File - See https://buttplug.io for more info.
//
// Copyright 2016-2022 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

use super::*;
#[cfg(feature = "serialize-json")]
use serde::{Deserialize, Serialize};

/// Generic command for setting a level (single magnitude value) of a device feature.
#[derive(Debug, Default, PartialEq, Clone)]
#[cfg_attr(feature = "serialize-json", derive(Serialize, Deserialize))]
pub struct ScalarSubcommand {
  #[cfg_attr(feature = "serialize-json", serde(rename = "Index"))]
  index: u32,
  #[cfg_attr(feature = "serialize-json", serde(rename = "Level"))]
  scalar: f64,
}

impl ScalarSubcommand {
  pub fn new(index: u32, scalar: f64) -> Self {
    Self { index, scalar }
  }

  pub fn index(&self) -> u32 {
    self.index
  }

  pub fn scalar(&self) -> f64 {
    self.scalar
  }
}

#[derive(Debug, Default, ButtplugDeviceMessage, PartialEq, Clone)]
#[cfg_attr(feature = "serialize-json", derive(Serialize, Deserialize))]
pub struct ScalarCmd {
  #[cfg_attr(feature = "serialize-json", serde(rename = "Id"))]
  id: u32,
  #[cfg_attr(feature = "serialize-json", serde(rename = "DeviceIndex"))]
  device_index: u32,
  #[cfg_attr(feature = "serialize-json", serde(rename = "Levels"))]
  scalars: Vec<ScalarSubcommand>,
}

impl ScalarCmd {
  pub fn new(device_index: u32, scalars: Vec<ScalarSubcommand>) -> Self {
    Self {
      id: 1,
      device_index,
      scalars,
    }
  }

  pub fn scalars(&self) -> &Vec<ScalarSubcommand> {
    &self.scalars
  }
}

impl ButtplugMessageValidator for ScalarCmd {
  fn is_valid(&self) -> Result<(), ButtplugMessageError> {
    self.is_not_system_id(self.id)?;
    for level in &self.scalars {
      self.is_in_command_range(
        level.scalar,
        format!(
          "Level {} for ScalarCmd index {} is invalid. Level should be a value between 0.0 and 1.0",
          level.scalar, level.index
        ),
      )?;
    }
    Ok(())
  }
}
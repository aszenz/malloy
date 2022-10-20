/*
 * Copyright 2021 Google LLC
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 */

import * as fs from "fs";
import * as path from "path";
import * as os from "os";

interface ComposerConfig {
  modelsPath: string;
}

export async function getConfig(): Promise<ComposerConfig> {
  let config = {
    modelsPath: path.resolve(process.cwd(), process.env.MODELS || ""),
  };
  const configFilePath = path.resolve("./composer_config.json");
  if (fs.existsSync(configFilePath)) {
    try {
      const file = fs.readFileSync(configFilePath, "utf8");
      const fileConfig = JSON.parse(file);
      config = { ...config, ...fileConfig };
    } catch (error) {
      // eslint-disable-next-line no-console
      console.log(error);
    }
  }

  if (config.modelsPath.startsWith("~")) {
    config.modelsPath = config.modelsPath.replace(/^~/, os.homedir());
  }

  config.modelsPath = path.resolve(config.modelsPath);
  return config;
}

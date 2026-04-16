/*
 * Copyright 2024, Nextflow Contributors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nextflow.bacalhau

import groovy.transform.CompileStatic
import org.pf4j.Plugin
import org.pf4j.PluginWrapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Nextflow plugin for Bacalhau distributed compute integration.
 *
 * Uses an explicit SLF4J logger instead of {@code @Slf4j}, because the
 * parent {@link Plugin} class already declares a {@code log} field and
 * {@code @Slf4j} would collide.
 *
 * @author Nextflow Contributors
 */
@CompileStatic
class BacalhauPlugin extends Plugin {

    private static final Logger LOGGER = LoggerFactory.getLogger(BacalhauPlugin)

    BacalhauPlugin(PluginWrapper wrapper) {
        super(wrapper)
    }

    @Override
    void start() {
        LOGGER.info("Bacalhau plugin started")
    }

    @Override
    void stop() {
        LOGGER.info("Bacalhau plugin stopped")
    }
}
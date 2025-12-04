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
import groovy.util.logging.Slf4j
import org.pf4j.Plugin
import org.pf4j.PluginWrapper

/**
 * Nextflow plugin for Bacalhau distributed compute integration
 *
 * @author Nextflow Contributors
 */
@Slf4j
@CompileStatic
class BacalhauPlugin extends Plugin {

    BacalhauPlugin(PluginWrapper wrapper) {
        super(wrapper)
    }

    @Override
    void start() {
        log.info("Bacalhau plugin started")
    }

    @Override
    void stop() {
        log.info("Bacalhau plugin stopped")
    }
}
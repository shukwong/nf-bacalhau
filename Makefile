# Convenience wrappers matching the nf-plugin-template convention.

# Build the plugin
assemble:
	./gradlew assemble

clean:
	rm -rf .nextflow*
	rm -rf work
	rm -rf build
	./gradlew clean

# Run plugin unit tests
test:
	./gradlew test

# Integration tests (requires live Bacalhau cluster)
integration-test:
	./gradlew integrationTest

# Install the plugin into local nextflow plugins dir (~/.nextflow/plugins)
install:
	./gradlew install

# Publish the plugin to the Nextflow Plugin Registry.
# Requires npr.apiKey in $HOME/.gradle/gradle.properties, or the NPR_API_KEY env var.
release:
	./gradlew releasePlugin

.PHONY: assemble clean test integration-test install release

#!/usr/bin/env nextflow

/*
 * Simple Hello World example for Bacalhau executor
 */

// Enable the Bacalhau plugin
plugins {
    id 'nf-bacalhau'
}

// Configure the executor
process {
    executor = 'bacalhau'
}

// Define the process
process sayHello {
    container 'ubuntu:latest'
    
    input:
    val greeting
    
    output:
    stdout
    
    script:
    """
    echo "${greeting} from Bacalhau distributed compute!"
    echo "Running on node: \$(hostname)"
    echo "Current time: \$(date)"
    """
}

// Define the workflow
workflow {
    greetings = Channel.of('Hello', 'Hola', 'Bonjour', 'Guten Tag', 'こんにちは')
    sayHello(greetings) | view
}
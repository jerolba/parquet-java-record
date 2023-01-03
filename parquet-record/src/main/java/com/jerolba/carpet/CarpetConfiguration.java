package com.jerolba.carpet;

public class CarpetConfiguration {

    private final AnnotatedLevels annotatedLevels;

    public CarpetConfiguration(AnnotatedLevels annotatedLevels) {
        this.annotatedLevels = annotatedLevels;
    }

    public AnnotatedLevels annotatedLevels() {
        return annotatedLevels;
    }

}

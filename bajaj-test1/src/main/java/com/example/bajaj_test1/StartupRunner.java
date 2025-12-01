package com.example.bajaj_test1;

import com.example.bajaj_test1.service.HiringService;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class StartupRunner {

    private final HiringService service;

    public StartupRunner(HiringService service) {
        this.service = service;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void runAfterStartup() {
        service.runFlowOnStartup();
    }
}

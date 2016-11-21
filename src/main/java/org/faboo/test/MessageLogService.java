package org.faboo.test;

import org.springframework.stereotype.Service;

/**
 */
@Service
public class MessageLogService {

    public void logMessage(String message) {
        System.out.println(message);
    }
}

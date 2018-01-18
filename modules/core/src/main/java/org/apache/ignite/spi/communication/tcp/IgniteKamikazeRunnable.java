package org.apache.ignite.spi.communication.tcp;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.communication.CommunicationSpi;

public class IgniteKamikazeRunnable implements IgniteRunnable {
    public UUID[] nodesToKill;

    @IgniteInstanceResource
    private Ignite ignite;

    @Override public void run() {
        BlockTcpCommunicationSpi communicationSpi = (BlockTcpCommunicationSpi)ignite.configuration().getCommunicationSpi();
        IgniteLogger log = communicationSpi.log;
        log.debug("[TCP]Before Killing " + nodesToKill + " on " + ignite.cluster().localNode().id());

        if (nodesToKill == null) {
            log.debug("[TCP]Killing " + nodesToKill + " on " + ignite.cluster().localNode().id());

            System.exit(0);
        }

        for (UUID uuid : nodesToKill) {
            if (ignite.cluster().localNode().id().equals(uuid)) {
                log.debug("[TCP]Killing " + nodesToKill + " on " + ignite.cluster().localNode().id());

                System.exit(0);
            }
        }
    }
}

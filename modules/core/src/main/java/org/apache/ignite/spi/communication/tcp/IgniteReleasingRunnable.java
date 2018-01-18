package org.apache.ignite.spi.communication.tcp;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.communication.CommunicationSpi;

public class IgniteReleasingRunnable implements IgniteRunnable {
    public UUID[] nodeWhereBlock;

    @IgniteInstanceResource
    private Ignite ignite;

    @Override public void run() {

        for (UUID uuid : nodeWhereBlock) {
            if (ignite.cluster().localNode().id().equals(uuid)) {
                BlockTcpCommunicationSpi communicationSpi = (BlockTcpCommunicationSpi)ignite.configuration().getCommunicationSpi();

                communicationSpi.unblockMessages();

                IgniteLogger log = communicationSpi.log;

                if (log != null)
                    log.debug("    [TCP] Msg unblocked succesfully! on " + ignite.cluster().localNode().id() + "\n");
            }
        }
    }
}

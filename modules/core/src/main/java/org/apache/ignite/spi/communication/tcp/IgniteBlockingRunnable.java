package org.apache.ignite.spi.communication.tcp;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.communication.CommunicationSpi;

public class IgniteBlockingRunnable implements IgniteRunnable {

    public UUID []nodeWhereBlock;

    public Class msgToBlock;

    public boolean waitUnblock;

    @IgniteInstanceResource
    private Ignite ignite;

    @Override public void run() {
        BlockTcpCommunicationSpi communicationSpi = (BlockTcpCommunicationSpi)ignite.configuration().getCommunicationSpi();
        IgniteLogger log = communicationSpi.log;

        log.debug("[TCP]Before blocking on node " + ignite.cluster().localNode().id() + "\n nodeWhereBlock= " + nodeWhereBlock);

        for (UUID uuid : nodeWhereBlock)
            if (ignite.cluster().localNode().id().equals(uuid)) {
                try {

                    communicationSpi.blockMessage(msgToBlock, waitUnblock);

                    if (log != null)
                        log.debug("    [TCP] Msg blocked succesfully! " + msgToBlock + "  on  " + ignite.cluster().localNode().id() + "\n");
                    else
                        System.out.println("[TCP] log is null! ");
                }
                catch (Throwable e) {
                    System.out.println("[TCP] EXCEPTION!" + e);
                }
            }
    }
}

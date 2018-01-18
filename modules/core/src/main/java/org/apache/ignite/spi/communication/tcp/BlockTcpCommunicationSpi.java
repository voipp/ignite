package org.apache.ignite.spi.communication.tcp;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;

public class BlockTcpCommunicationSpi extends TcpCommunicationSpi {
    /** */
    volatile Class msgCls;

    /** Message latch. */
    private CountDownLatch msgLatch;

    @LoggerResource(categoryName = "BlockTcpCommunicationSpi")
    public IgniteLogger log;

    /** {@inheritDoc} */
    @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
        throws IgniteSpiException {
        Class msgCls0 = msgCls;

        try {

            log.debug("    [TCP SPI]before msg sent from " + ignite().cluster().localNode().id() + " to " + node.id()
                + " payload: " + msg + "\n");

            if (msgCls0 != null && msg instanceof GridIoMessage
                && ((GridIoMessage)msg).message().getClass().equals(msgCls)) {

                log.debug("    [TCP SPI]Msg was blocked " + msg + "\n on" + ignite.cluster().localNode().id());

                try {
                    if (msgLatch != null) {
                        log.debug("[TCP SPI]Before waiting on latch on " + ignite.cluster().localNode().id());

                        msgLatch.await();

                        log.debug("[TCP SPI]After waiting on latch on " + ignite.cluster().localNode().id() + " so now, resume sending " + msg + " to " + node.id());
                    }
                    else
                        return;
                }
                catch (InterruptedException e) {
                    assert false;
                }
            }

            log.debug("    [TCP SPI]!msg sent from " + ignite().cluster().localNode().id() + " to " + node.id()
                + " payload: " + msg + "\n");

            super.sendMessage(node, msg, ackC);
        }catch (Throwable e){
            log.error("error on " + ignite().cluster().localNode().id() + " while sending the message " + msg + "\n to" + node.id(),e);
        }
    }

    /**
     * @param clazz Class of messages which will be block.
     */
    public void blockMessage(Class clazz, boolean waitUnblock) {
        msgCls = clazz;

        if (waitUnblock)
            msgLatch = new CountDownLatch(1);
        else
            msgLatch = null;
    }

    /**
     * Unlock all message.
     */
    public void unblockMessages() {
        msgCls = null;

        if (msgLatch != null)
            msgLatch.countDown();
    }
}

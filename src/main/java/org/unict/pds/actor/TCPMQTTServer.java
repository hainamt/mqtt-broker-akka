package org.unict.pds.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.io.Tcp;
import akka.io.TcpMessage;
import org.unict.pds.actor.server.MQTTManager;
import org.unict.pds.configuration.ConfigurationExtension;
import org.unict.pds.configuration.TCPConfiguration;

import java.net.InetSocketAddress;


public class TCPMQTTServer extends AbstractActor {

    @Override
    public void preStart() throws Exception {
        TCPConfiguration tcpConfiguration = ConfigurationExtension.getInstance()
                .get(getContext().getSystem()).tcpConfig();
        final ActorRef tcp = Tcp.get(getContext().getSystem()).manager();
        Tcp.Command bindCommand = TcpMessage.bind(
                getSelf(),
                new InetSocketAddress(
                        tcpConfiguration.hostname(),
                        tcpConfiguration.port()),
                        tcpConfiguration.backlog());
        tcp.tell(bindCommand, getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(
                        Tcp.Bound.class,
                        msg -> System.out.println("Server bound to: " + msg.localAddress()))
                .match(
                        Tcp.CommandFailed.class,
                        msg -> getContext().stop(getSelf())
                )
                .match(
                        Tcp.Connected.class,
                        conn -> {
                            System.out.println("Client connected from: " + conn.remoteAddress());
                            ActorRef tcpConnection = getSender();
                            final ActorRef handler = getContext().actorOf(Props.create(MQTTManager.class, tcpConnection));
                            getSender().tell(TcpMessage.register(handler), getSelf());
                        }
                )
                .build();
    }
}
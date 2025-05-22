package org.unict.pds.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.io.Tcp;
import akka.io.TcpMessage;
import org.unict.pds.actor.mqtt.MQTTHandler;

import java.net.InetSocketAddress;


public class TCPServer extends AbstractActor {

    public static Props props() {
        return Props.create(TCPServer.class);
    }
    @Override
    public void preStart() throws Exception {
        final ActorRef tcp = Tcp.get(getContext().getSystem()).manager();
        Tcp.Command bindCommand = TcpMessage.bind(getSelf(),
                new InetSocketAddress("localhost", 8883), 100);
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
                            final ActorRef handler = getContext().actorOf(Props.create(MQTTHandler.class));
                            getSender().tell(TcpMessage.register(handler), getSelf());
                        }
                )
                .build();
    }
}
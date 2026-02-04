module clients {
    requires org.slf4j;
    requires kafka.clients;
    requires com.fasterxml.jackson.annotation;
    requires org.fusesource.jansi;
    requires com.fasterxml.jackson.datatype.jdk8;
    requires com.fasterxml.jackson.datatype.jsr310;
    requires com.fasterxml.jackson.databind;
    requires static lombok;

    exports no.nav.kafka.sandbox.assignor;
}
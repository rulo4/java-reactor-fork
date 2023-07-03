package com.rp;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.util.stream.Stream;

public class ReactorFileReader {

    public static void main(String[] args) throws IOException {
        final var start = System.currentTimeMillis();

        Path ipPath = Paths.get(ReactorFileReader.class.getClassLoader().getResource("input-file.csv").getPath());

        Flux<String> stringFlux = Flux.using(
                () -> Files.lines(ipPath),
                Flux::fromStream,
                Stream::close
        );

        final var processedFlux = stringFlux
                .parallel(2)
                .runOn(Schedulers.parallel())
                .map(ReactorFileReader::processRow)
//                .sequential()
//                .publishOn(Schedulers.single())
//                .subscribe(
//                        System.out::println,
//                        System.out::println,
//                        () -> System.out.println("TOTAL: " + (System.currentTimeMillis() - start))
//                );
        ;


        Path opPath = Paths.get(ReactorFileReader.class.getClassLoader().getResource("").getPath() + "output-file.txt");
        BufferedWriter bw = Files.newBufferedWriter(opPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        processedFlux
                .doOnNext(s -> write(bw, s))
                .sequential()
                .publishOn(Schedulers.single())
                .doFinally(signalType -> close(bw))
                .blockLast();

//        sleep(100000);
    }

    private static String processRow(String s) {
        System.out.println(s + " PROCESSING...");
            final var sleepTime = (long) Math.floor(Math.random() * 1000 * 6 + 2);
            sleep(sleepTime);
            return "READ: " + s + " took " + sleepTime;
    }

    private static void sleep(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    private static void close(Closeable closeable){
        try {
            closeable.close();
            System.out.println("Closed the resource");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void write(BufferedWriter bw, String string){
        System.out.println(string + " WRITING...");
        try {
            bw.write(string + " - " + LocalDateTime.now());
            bw.newLine();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

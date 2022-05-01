package mxq.echooy.reactor.core;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/***
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class FluxExamples {

    @Test
    // 创建一个没有元素的流，订阅时只发布OnComplete事件
    public void empty() {
        Flux<Void> source = Flux.empty();
        StepVerifier.create(source)
                .verifyComplete();
    }

    @Test
    // 创建一个只下发onError事件的流
    public void error() {
        StepVerifier.create(Flux.error(new IllegalStateException("Boom")))
                .expectError(IllegalStateException.class)
                .verify();
    }

    @Test
    public void just() {
        Flux<String> source = Flux.just("1", "2", "3");
        StepVerifier.create(source)
                .expectNext("1")
                .expectNext("2")
                .expectNext("3")
                .verifyComplete();
    }

    @Test
    public void from() {
        Flux<String> source = Flux.from(Flux.just("1", "2", "3"));
        StepVerifier.create(source)
                .expectNext("1")
                .expectNext("2")
                .expectNext("3")
                .verifyComplete();
    }

    @Test
    // 根据Stream创建流
    public void fromStream() {
        Flux<String> source = Flux.fromStream(Stream.of("1", "2", "3"));
        StepVerifier.create(source)
                .expectNext("1")
                .expectNext("2")
                .expectNext("3")
                .verifyComplete();
    }

    @Test
    // 根据Iterable创建流，例如：List、Set、Queue等
    public void fromIterable() {
        Flux<String> source = Flux.fromIterable(List.of("1", "2", "3"));
        StepVerifier.create(source)
                .expectNext("1")
                .expectNext("2")
                .expectNext("3")
                .verifyComplete();
    }

    @Test
    // 根据数值范围创建流range(n,m) => n -> n+m-1
    public void range() {
        Flux<Integer> source = Flux.range(0, 3);
        StepVerifier.create(source)
                .expectNext(0)
                .expectNext(1)
                .expectNext(2)
                .verifyComplete();
    }

    @Test
    // 创建一个依靠其它线程周期产生元素的流，正常情况下，流永远不会结束
    public void interval() {
        Flux<Long> source = Flux.interval(Duration.of(1, ChronoUnit.SECONDS));
        StepVerifier.create(source)
                .expectNext(0L)
                .expectNext(1L)
                .expectNext(2L)
                .thenCancel()
                .verify();
    }

    @Test
    // 使用generate创建流
    public void generate() {
        Flux<String> source = Flux.generate(sink -> {
            sink.next("1"); // 执行过程中只能调用最多一次sink#next
            sink.complete();
        });
        StepVerifier.create(source)
                .expectNext("1")
                .verifyComplete();
    }

    @Test
    // 使用generate创建流，可以进行状态设置
    public void generateWithStateV1() {
        final Random random = new Random();
        Flux<Integer> source = Flux.generate(ArrayList::new, (list, sink) -> {
            int value = random.nextInt(100);
            list.add(value);
            sink.next(value);
            // 仅创建50个元素
            if (list.size() == 50) {
                sink.complete();
            }
            return list;
        });
        StepVerifier.create(source)
                .expectNextCount(50)
                .verifyComplete();
    }

    @Test
    // 使用generate创建流，可以进行状态设置和流终止时的状态回调
    public void generateWithStateV2() {
        final Random random = new Random();
        Flux<Integer> source = Flux.generate(ArrayList::new,
                (list, sink) -> {
                    int value = random.nextInt(100);
                    list.add(value);
                    sink.next(value);
                    if (list.size() == 50) {
                        sink.complete();
                    }
                    return list;
                }, ArrayList::clear);
        StepVerifier.create(source)
                .expectNextCount(50)
                .verifyComplete();
    }

    @Test
    // 使用create创建流，可以使用同步sink多次或多线程sink#next(即多个生产者)
    public void create() {
        Flux<Integer> source = Flux.create(sink -> {
            IntStream.range(0, 100)
                    .parallel()
                    .forEach(sink::next);
            sink.complete();
        });
        StepVerifier.create(source)
                .expectNextCount(100)
                .verifyComplete();
    }

    @Test
    // 使用push创建流，单线程sink#next(即单个生产者)才能保证线程安全(元素数量大小一致)
    public void push() {
        Flux<Integer> source = Flux.push(sink -> {
            for (int i = 0; i < 100; i++) {
                sink.next(i);
            }
            sink.complete();
        });
        StepVerifier.create(source)
                .expectNextCount(100)
                .verifyComplete();
    }

    @Test
    // 使用using创建流
    public void using() {
        Random random = new Random();
        Flux<?> source = Flux.using(ArrayList::new, list -> {
            for (int i = 0; i < 100; i++) {
                int value = random.nextInt(100);
                list.add(value);
            }
            return Flux.fromIterable(list);
        }, ArrayList::clear);
        StepVerifier.create(source)
                .expectNextCount(100)
                .verifyComplete();
    }

    @Test
    // 使用usingWhen，可根据一个Publisher创建流，创建的流Complete时将触发cleanUp回调
    public void usingWhen() {
        Flux<String> source = Flux.usingWhen(Mono.just("a"),
                e -> Mono.just(e.toUpperCase()),
                e -> Mono.never());
        StepVerifier.create(source)
                .expectNext("A")
                .thenCancel()
                .verify();
    }

    @Test
    // 重复订阅原始流下发到下游
    public void repeat() {
        Flux<String> source = Flux.just("1", "2", "3")
                .repeat(1);
        StepVerifier.create(source)
                .expectNext("1")
                .expectNext("2")
                .expectNext("3")
                .expectNext("1")
                .expectNext("2")
                .expectNext("3")
                .verifyComplete();
    }

    @Test
    // 过滤元素中的流
    public void filter() {
        Flux<String> source = Flux.just("1", "2", "3")
                .filter(e -> e.equals("2"));
        StepVerifier.create(source)
                .expectNext("2")
                .verifyComplete();
    }

    @Test
    // 根据某个流来过滤流
    public void filterWhen() {
        Flux<String> flux1 = Flux.just("1", "2", "3");
        Flux<String> flux2 = Flux.just("2");

        Flux<String> source = flux1.filterWhen(flux2::hasElement);
        StepVerifier.create(source)
                .expectNext("2")
                .verifyComplete();
    }

    @Test
    // 获取流的前N个元素
    public void take() {
        Flux<String> source = Flux.just("1", "2", "3")
                .take(2);
        StepVerifier.create(source)
                .expectNext("1")
                .expectNext("2")
                .verifyComplete();
    }

    @Test
    // 获取流的后N个元素
    public void takeLast() {
        Flux<String> source = Flux.just("1", "2", "3")
                .takeLast(2);
        StepVerifier.create(source)
                .expectNext("2")
                .expectNext("3")
                .verifyComplete();
    }

    @Test
    // 一直下发元素到满足Predicate条件时(包含满足该条件的当前元素)，将不再继续下发元素
    public void takeUntil() {
        Flux<String> source = Flux.just("1", "2", "3")
                .takeUntil(e -> e.equals("2"));
        StepVerifier.create(source)
                .expectNext("1")
                .expectNext("2")
                .verifyComplete();
    }

    @Test
    // 只有满足Predicate条件时(只要遇到False都将终止流)，才下发元素
    public void takeWhile() {
        Flux<String> source = Flux.just("1", "2", "3");
        StepVerifier.create(source.takeWhile(s -> s.equals("1"))).expectNext("1").verifyComplete();
        StepVerifier.create(source.takeWhile(s -> s.equals("2"))).verifyComplete();
    }

    @Test
    // 流按固定大小将元素分批下发到下游
    public void buffer() {
        Flux<List<String>> source = Flux.just("1", "2", "3").buffer(2);
        StepVerifier.create(source)
                .expectNext(List.of("1", "2"))
                .expectNext(List.of("3"))
                .verifyComplete();
    }

    @Test
    // 根据Predicate缓冲元素列表再下发到下游,可根据cutBefore参数选择是否丢弃缓冲元素
    public void bufferUntil() {
        Flux<List<String>> flux = Flux.just("1", "2", "3")
                .bufferUntil(e -> e.equals("2"), false);
        StepVerifier.create(flux)
                .expectNext(List.of("1", "2"))
                .expectNext(List.of("3"))
                .verifyComplete();
    }

    @Test
    // 根据Predicate缓冲满足条件的元素到列表中，下发到下游
    public void bufferWhile() {
        Flux<List<String>> source = Flux.just("1", "2", "3").bufferWhile(e -> e.equals("2"));
        StepVerifier.create(source)
                .expectNext(List.of("2"))
                .verifyComplete();
    }

    @Test
    // 将流根据window大小拆分为多个子流作为元素进行下发
    public void window() {
        Flux<Flux<String>> source = Flux.just("1", "2", "3").window(2);
        assertThat(source.concatMap(Flux::collectList)
                .collectList()
                .block()).containsExactly(Arrays.asList("1", "2"), Collections.singletonList("3"));
    }

    @Test
    // 根据Predicate条件，断言为True时将包含当前及前面的元素拆分为子流进行下发
    public void windowUntil() {
        Flux<Flux<String>> source = Flux.just("1", "2", "3")
                .windowUntil(e -> e.equals("2"));
        assertThat(source.concatMap(Flux::collectList)
                .collectList()
                .block()).containsExactly(Arrays.asList("1", "2"), Collections.singletonList("3"));
    }

    @Test
    // 根据Predicate条件，遇到False时将满足条件的元素拆分为子流进行下发
    public void windowWhile() {
        Flux<Flux<String>> source = Flux.just("1", "2", "3")
                .windowWhile(e -> !e.equals("2"));
        assertThat(source.concatMap(Flux::collectList)
                .collectList()
                .block())
                .containsExactly(Collections.singletonList("1"), Collections.singletonList("3"));
    }

    @Test
    // 将流中的元素进行合并为Stream后下发
    public void collect() {
        Mono<List<String>> source1 = Flux.just("1", "2", "3")
                .collect(Collectors.toList());
        assertThat(source1.block()).containsAll(List.of("1", "2", "3"));

        Mono<List<String>> source2 = Flux.just("1", "2", "3")
                .collectList();
        assertThat(source2.block()).containsAll(List.of("1", "2", "3"));
    }

    @Test
    // 将多个子流合并为一个流，不同流之间元素可能交错，如想保证流顺序，使用mergeSequential
    public void merge() {
        Flux<String> source = Flux.merge(Flux.just("1", "2"), Flux.just("3"));
        StepVerifier.create(source)
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    // 将多个子流合并为一个流，并保证不同流之间元素的顺序
    public void mergeSequential() {
        Flux<String> source = Flux.mergeSequential(Flux.just("1", "2"), Flux.just("3"));
        StepVerifier.create(source)
                .expectNext("1")
                .expectNext("2")
                .expectNext("3")
                .verifyComplete();
    }

    @Test
    // 一个流合并另一个流，等同于Flux#megre，方法参数大小为2时
    public void mergeWith() {
        Flux<String> source1 = Flux.just("1", "2");
        Flux<String> source2 = Flux.just("3");
        Flux<String> source = source1.mergeWith(source2);
        StepVerifier.create(source)
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    // 合并流的元素
    public void reduce() {
        Mono<Integer> source = Flux.just(1, 2, 3).reduce(Integer::sum);
        StepVerifier.create(source)
                .expectNext(6)
                .verifyComplete();
    }

    @Test
    // 合并流的元素(包含初始值)
    public void reduceWith() {
        Mono<Integer> source = Flux.just(1, 2, 3).reduceWith(() -> 100, Integer::sum);
        StepVerifier.create(source)
                .expectNext(106)
                .verifyComplete();
    }

    @Test
    // 合并两个流之间的元素为元组进行下发
    public void zipWith() {
        Flux<Tuple2<String, String>> source1 = Flux.just("a", "b")
                .zipWith(Flux.just("c", "d", "e"));
        StepVerifier.create(source1)
                .expectNext(Tuples.of("a", "c"))
                .expectNext(Tuples.of("b", "d"))
                .verifyComplete();

        Flux<String> source2 = Flux.just("a", "b")
                .zipWith(Flux.just("c", "d", "e"), (s1, s2) -> s1 + s2);// 可对元组进行再操作

        StepVerifier.create(source2)
                .expectNext("ac")
                .expectNext("bd")
                .verifyComplete();
    }

    @Test
    // 转换流的元素，并不保证元素的顺序，在意顺序使用concatMap
    public void flatmap() {
        Map<String, String> store = Map.of("1", "a", "2", "b");
        Flux<String> source = Flux.just("1", "2")
                .flatMap(e -> Flux.just(store.get(e)));
        StepVerifier.create(source)
                .expectNextCount(2)
                .verifyComplete();
    }

    @Test
    // 转换流的元素，保证元素的顺序
    public void concatMap() {
        Map<String, String> store = Map.of("1", "a", "2", "b");
        Flux<String> source = Flux.just("1", "2")
                .concatMap(e -> Flux.just(store.get(e)));
        StepVerifier.create(source)
                .expectNext("a")
                .expectNext("b")
                .verifyComplete();
    }

    @Test
    // 将源流转换为目标流再下发到下游，可以将多个对流的操作如map、filter结合作为Function传入
    public void transform() {
        // 将流的元素变成大写，且过滤掉元素b
        Function<Flux<String>, Flux<String>> function = f -> f.filter(color -> !color.equals("b")).map(String::toUpperCase);
        Flux<String> source = Flux.just("a", "b", "c")
                .transform(function);
        StepVerifier.create(source)
                .expectNext("A")
                .expectNext("C")
                .verifyComplete();
    }

    @Test
    // 使用sink主动下发流的元素
    public void handle() {
        Flux<String> source = Flux.just("a", "b", "c")
                .handle((s, sink) -> sink.next(s.toUpperCase()));
        StepVerifier.create(source)
                .expectNext("A")
                .expectNext("B")
                .expectNext("C")
                .verifyComplete();
    }

    @Test
    // 将流转换为第一个元素和原始流的组合，通常可根据第一个元素决定相关操作
    public void switchOnFirst() {
        Flux<String> source = Flux.just("a", "b", "c")
                .switchOnFirst(((firstVal, flux) -> {
                    String val = firstVal.get();
                    if ("a".equals(val)) {
                        return flux.take(2); // 如果流以a开头，只获取前两个元素，否则返回原始流
                    }
                    return flux;
                }));
        StepVerifier.create(source)
                .expectNext("a")
                .expectNext("b")
                .verifyComplete();
    }

    @Test
    // 忽略流的元素
    public void then() {
        Mono<Void> source1 = Flux.just("1")
                .then();
        StepVerifier.create(source1)
                .verifyComplete();

        Flux<String> flux = Flux.just("1")
                .thenMany(Flux.just("a"));
        StepVerifier.create(flux)
                .expectNext("a")
                .verifyComplete();
    }

}

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fzzy/radix/redis"
)

var (
	key         = flag.String("key", "test_queue", "the redis list key")
	total       = flag.Int("total", 100, "totoal msg to push")
	delay       = flag.Duration("delay", 1*time.Second, "push delay")
	concurrency = flag.Int("c", 10, "concurrency")
)

func errHandler(err error) {
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}

func main() {
	flag.Parse()

	done := make(chan struct{})
	stopSigs := make(chan os.Signal, 1)
	signal.Notify(stopSigs, syscall.SIGINT, syscall.SIGTERM)

	for i := 0; i < *concurrency; i++ {
		name := fmt.Sprintf("producer_%d", i)
		go publish(name)
	}

	go func() {
		sig := <-stopSigs
		fmt.Println("receive signal:", sig)
		done <- struct{}{}
	}()

	<-done
	fmt.Println("done")
}

func publish(name string) {
	flag.Parse()
	client, err := redis.DialTimeout("tcp", "127.0.0.1:6379", 10*time.Second)
	errHandler(err)
	defer client.Close()

	for i := 0; i <= *total; i++ {
		value := fmt.Sprintf("%s_%d", name, i)
		reply := client.Cmd("lpush", *key, value)
		errHandler(reply.Err)
		fmt.Println(name, " lpush: ", value)
		time.Sleep(*delay)
	}
	fmt.Println("done")
}

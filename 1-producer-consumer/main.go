//////////////////////////////////////////////////////////////////////
//
// Given is a producer-consumer scenario, where a producer reads in
// tweets from a mockstream and a consumer is processing the
// data. Your task is to change the code so that the producer as well
// as the consumer can run concurrently
//

package main

import (
	"fmt"
	"time"
)

func producer(stream_ch <-chan Stream) <-chan *Tweet {
	tweet_ch := make(chan *Tweet)
	go func() {
		defer close(tweet_ch)
		for stream := range stream_ch {
			for {
				tweet, err := stream.Next()
				if err == ErrEOF {
					break
				}
				tweet_ch <- tweet
			}
		}
	}()

	return tweet_ch
}

func consumer(tweet_ch <-chan *Tweet) {
	for t_ch := range tweet_ch {
		if t_ch.IsTalkingAboutGo() {
			fmt.Println(t_ch.Username, "\ttweets about golang")
		} else {
			fmt.Println(t_ch.Username, "\tdoes not tweet about golang")
		}
	}
}

func main() {
	start := time.Now()
	ch := make(chan Stream, 1)
	go func() {
		ch <- GetMockStream()
		close(ch)
	}()

	// Producer
	tweets := producer(ch)

	// Consumer
	consumer(tweets)

	fmt.Printf("Process took %s\n", time.Since(start))
}

(ns twittersimilarities.wordvectors
   (:use cascalog.api
	 cascalog.checkpoint
	 twittersimilarities.tweetops)
   (:require 
	[cascalog [vars :as v] [ops :as c]]
	[clojure.string :as cs]
	[twittersimilarities.utils :as su]
	[twittersimilarities.ops :as o]
	[twittersimilarities.tweetops :as to]
	[twittersimilarities.readers :as r]
	)
   (:gen-class))







(defn create-partial-vector [tweet-loc output]	
	(with-job-conf {"mapred.job.priority" "VERY_LOW"}
	(let [source (su/globhfs-textfile tweet-loc)]
	        (let [word-vec (<- [?date-word ?count] (source ?line) (o/extract 8 #"[^\t]+" ?line  :> ?year ?month ?day _ _ _ ?tweet ?timezone)
			        (valid-tweet ?timezone)
				(make-date ?year ?month ?day :> ?dt)
				(to/fix-date ?dt :> ?date)
				(o/split ?tweet :> ?word)
				(cs/lower-case ?word :> ?word)
				(cs/replace ?word #"[^a-z\-\_]" "" :> ?word)
				(str ?date "|" ?word :> ?date-word)
				(no-repeats ?word)
				(valid-word ?word)
				(valid-length ?word)
				(c/count ?count))
			] 
			(?<- "Create-partial-word-vector" (hfs-textline output) [?date ?word ?count] (word-vec ?date-word ?count) 
			(o/extract 2 #"[^\|]+" ?date-word :> ?date ?word) 
			;(str ?date "\t" ?word "\t" ?count :> ?out-line)
			(:distinct true))
		)
	))
)
(defn compute-date-word-total [vector-path output]
	(with-job-conf {"mapred.job.priority" "VERY_LOW"}
	(let [word_vec (hfs-textline vector-path)]
		(let [sums 	(<- [?date ?sum] (word_vec ?line) (o/extract 3 #"[^\t]+" ?line :> ?date _ ?count) (o/to-int ?count :> ?icount) (c/sum ?icount :> ?sum))]
		(?<- "Getting-total-words-for-date" (hfs-textline output) [?date ?sum] (sums ?date ?sum) 
		 ;(str ?date "\t" ?sum :> ?out-line) 
		 (:distinct true))
		)
	))
)
(defn combine-into-true-word-vector [vector-path sum-path output]
	(with-job-conf {"mapred.job.priority" "VERY_LOW"}
		(let [word-vec (r/read-from-partial-word-vec vector-path)]
			(let [date-sums (r/read-from-totals sum-path)]
				(let [true-vec 	(<- [?date ?word ?count ?norm ?sum] 
					(word-vec ?date ?word ?count)
					(date-sums ?date ?sum)
					(o/to-int ?count :> ?icount)
					(o/to-int ?sum :> ?isum)
					(div ?icount ?isum :> ?norm)	
					)]
					(?<- "Combine-sums-norms-and-word-frequencies"  (hfs-textline output) [?date ?word ?count ?norm ?sum] 
					(true-vec ?date ?word ?count ?norm ?sum)	
					;(str ?date "\t" ?word "\t" ?count "\t" ?norm "\t" ?sum :> ?out-line)
					(:distinct true))
				)
			)
		))
)
	

(defn build-tweet-vector [tweet-loc word-vector-loc]
	(println (str "**Building tweet word vectors into " word-vector-loc " from " tweet-loc))
	(workflow ["/tmp/max/twitter_vector_tmp"]
		stage-1 ([:tmp-dirs partial-tweet-vector-step]
			(create-partial-vector tweet-loc partial-tweet-vector-step)
			)
		stage-2 ([:tmp-dirs date-word-sums]
			(compute-date-word-total partial-tweet-vector-step date-word-sums) ;At some point this can be replaced with a ??<-
			)
		stage-3 ([:deps [stage-1 stage-2]]
			(combine-into-true-word-vector partial-tweet-vector-step date-word-sums word-vector-loc)
			)		
	)
)


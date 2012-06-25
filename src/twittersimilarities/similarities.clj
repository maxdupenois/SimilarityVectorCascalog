(ns twittersimilarities.similarities
   (:use cascalog.api
	cascalog.checkpoint)
   (:require 
	[cascalog [vars :as v] [ops :as c]]
	[twittersimilarities.utils :as su]
	[twittersimilarities.ops :as o]
	[twittersimilarities.readers :as r]
	[twittersimilarities.tweetops :as to]

	)
   (:gen-class))


(defn words-for-dates [word-vec]
(let [
	words (<- [?dummy ?w] (word-vec _ ?w _ _ _) (identity 1 :> ?dummy) (:distinct true))
	dates (<- [?dummy ?d] (word-vec ?d _ _ _ _) (identity 1 :> ?dummy) (:distinct true))
	]
	(<- [!!w !!d] (words ?dummy !!w) (dates ?dummy !!d) )
)
)

(defn build-true-word-vec [word-vector-loc true-word-vec]
	(with-job-conf {"mapred.job.priority" "VERY_LOW"}
	(let [
			word-vec (r/read-from-word-vec word-vector-loc)
			;filled-zeroes (<- [?word ?date ?norm] ((words-for-dates word-vec) ?word ?date)  
			;				(word-vec ?date ?word _ !!n _) 
			;				(o/blank-to-zero !!n :> ?norm) )
	      ]
	  (?<- "Build-true-word-vector" (hfs-textline true-word-vec) [?word ?date ?norm] ((words-for-dates word-vec) ?word ?date)
	          (word-vec ?date ?word _ !!n _) 
		  (o/blank-to-zero !!n :> ?norm) )
		;(?<- "Build-true-word-vector" (hfs-textline true-word-vec) [?outline]
		; (filled-zeroes ?word ?date ?norm) 
		;(str ?word "\t" ?date "\t" ?norm :> ?outline))
	)
	)
)

(def i 0)
(def n 0)
(defmapop get-i [d]
	(def i (+ i 1))
	(identity i)
)
(defmapop get-n [d]
	(def n (+ n 1))
	(identity n)
)
(defn index-dates[dates]
	(def n 0)
	(<- [?d ?i] (dates ?d) (get-n ?d :> ?i) (:distinct true))
)
(defn index-dates-lagged[dates]
	(def i 0)
	(<- [?d ?i] (dates ?d) (get-i ?d :> ?i) (:distinct true))
)

(defn build-curr-date-to-prev-date[word-vector-loc date-to-prev-date]
	(with-job-conf {"mapred.job.priority" "VERY_LOW"}
		(let [
			word-vec (r/read-from-word-vec word-vector-loc)
			dates (<- [?d] (word-vec ?d _ _ _ _) (:distinct true))
			dummy-partial (<-  [?blank] ([" "] ?blank) (:distinct true))
			dates-lagged (union dates dummy-partial)
			dates-lagged-indexed (index-dates-lagged dates-lagged)
			dates-indexed (index-dates dates)
			]
			(?<- "Building-map-of-current-date-to-previous" (hfs-textline date-to-prev-date) [?d1 ?d2] 
			(dates-indexed ?d1 ?i) 
			(dates-lagged-indexed ?d2 ?i)
			(:distinct true))
		)
	)	
)

(defn build-word-norms-vecs-by-date [true-word-vec word-norms-vecs-by-date]
	(with-job-conf {"mapred.job.priority" "VERY_LOW"}
		(let [word-vec (hfs-textline true-word-vec)] 
			(?<- "Building-vectors-of-word-norm-values-by-date" (hfs-textline word-norms-vecs-by-date :outfields ["?word" "?norm"] :templatefields "?date" :sink-template "%s") 
			[?date ?word ?norm] (word-vec ?line)
			(o/extract 3 #"[^\t]+" ?line :> ?word ?date ?norm)
			(:distinct true)) 
		)
	)
)
(defn build-similarity [current previous similarities-loc word-norms-vecs-by-date]	
	(let[
		current-norms (<- [?word ?norm] ((hfs-textline (str word-norms-vecs-by-date "/" current)) ?line) 
						(o/extract 2 #"[^\t]+" ?line :> ?word ?norm))
		previous-norms (<- [?word ?norm] ((hfs-textline (str word-norms-vecs-by-date "/" previous)) ?line) 
						(o/extract 2 #"[^\t]+" ?line :> ?word ?norm))
		job-name (str "Building-similarity-score-from-" previous "-to-" current)
		]
		(?<- "Building-similarity-score" 
			(hfs-textline similarities-loc :outfields ["?dt" "?similarity"] :templatefields "?dt" :sink-template "%s") 
			[?dt ?similarity]
			(current-norms ?word ?curr-norm-str)
			(previous-norms ?word ?prev-norm-str)
			(o/to-double ?curr-norm-str :> ?curr-norm)
			(o/to-double ?prev-norm-str :> ?prev-norm)
			(o/dot-product ?curr-norm ?prev-norm :> ?dp)
			(o/magnitude-sq ?curr-norm :> ?curr-mag-sq)
			(o/magnitude-sq ?prev-norm :> ?prev-mag-sq)
			(o/sqrt ?curr-mag-sq :> ?curr-mag)
			(o/sqrt ?prev-mag-sq :> ?prev-mag)
			(o/cosine-sim ?dp ?curr-mag ?prev-mag :> ?similarity)
			(identity current :> ?dt)
		)
	)
)

(defn build-similarities-keep-stages[word-vector-loc friendlyname] 
	(let [
		similarities-loc (str "/user/maxdupenois/twitter-similarities/dated=" friendlyname)
		individual-similarities-loc (str "/user/maxdupenois/twitter-similarities/tmp/individual-similarities/dated=" friendlyname)
		true-word-vec (str "/user/maxdupenois/twitter-similarities/tmp/true-word-vec/dated=" friendlyname)
		date-to-prev-date (str "/user/maxdupenois/twitter-similarities/tmp/date-to-prev-date/dated=" friendlyname)
		word-norms-vecs-by-date (str "/user/maxdupenois/twitter-similarities/tmp/word-norms-vecs-by-date/dated=" friendlyname)
		]

		(if  (not (su/hdp-path-exists? true-word-vec))
			(build-true-word-vec word-vector-loc true-word-vec)
		)
		(if  (not (su/hdp-path-exists? date-to-prev-date))
			(build-curr-date-to-prev-date word-vector-loc date-to-prev-date)
		)
		(if  (not (su/hdp-path-exists? word-norms-vecs-by-date))
			(build-word-norms-vecs-by-date true-word-vec word-norms-vecs-by-date)
		)
		; Okay, all the norms have been dumped into files within matrix-loc directory named by date
		; need to loop through all dates and their previous so:
		; date-to-prev (??<- [?date ?prev] ((hfs-textline date-to-prev-date) ?line) (extract 2 #"[^\t]+" ?line :> ?date ?prev))
		(with-job-conf {"mapred.job.priority" "VERY_LOW"}
		(if  (not (su/hdp-path-exists? individual-similarities-loc))
			(let [
				date-to-prev (??<- [?date ?prev] 
					((hfs-textline date-to-prev-date) ?line) 
				(o/extract 2 #"[^\t]+" ?line :> ?date ?prev))
				]
				(doseq [curr-prev date-to-prev] 
					(if (not (= " " (second curr-prev)))
						(build-similarity (first curr-prev) (second curr-prev) individual-similarities-loc word-norms-vecs-by-date)
					)
				)	
			)
		)
		; combine parts 
		(let [
			similarities (hfs-textline (str individual-similarities-loc "/*"))
			]
			(?<- "Combining-similarities" (hfs-textline similarities-loc) [?line] (similarities ?line) (:distinct true) )
		)
		)
	)
)












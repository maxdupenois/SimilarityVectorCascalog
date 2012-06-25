(ns twittersimilarities.tweetnewsvectors
  (:use cascalog.api
	 cascalog.checkpoint
	 twittersimilarities.tweetops)
   (:require 
	[twittersimilarities.newsscraper :as news]
	[twittersimilarities.utils :as su]
	[twittersimilarities.ops :as o]
	[twittersimilarities.wordvectors :as wv]
	[cascalog.ops :as c]
	[clojure.string :as cs]
	)
   (:gen-class))

(defmapop true-url [url]
  (news/get-true-url url))

(defn create-link-vector [tweet-loc output]	
	(with-job-conf {"mapred.job.priority" "VERY_LOW"}
	(let [source (su/globhfs-textfile tweet-loc)]
	        (let [link-vec 
		      (<- [?date-link ?count] (source ?line) (o/extract 8 #"[^\t]+" ?line  :> ?year ?month ?day _ _ _ ?tweet ?timezone)
			        (valid-tweet ?timezone)
				(make-date ?year ?month ?day :> ?dt)
				(fix-date ?dt :> ?date)
				(o/split ?tweet :> ?word)
				(cs/lower-case ?word :> ?word)
				(is-link? ?word)
				(true-url ?word :> ?link)
				(str ?date "|" ?link :> ?date-link) ;Combined so that the count is per-day
				(c/count ?count))
		      ] 
			(?<- "Create-link-vector" (hfs-textline output) [?date ?link ?count] (link-vec ?date-link ?count) 
			(o/extract 2 #"[^\|]+" ?date-link :> ?date ?link) 
			(:distinct true))
		)
	))
	)

(defmapop get-article-content [link]
  (let [article (news/get-article link)]
    (str (first (:headline article)) " " (cs/join " " (:text article)))
  ))

(defn create-word-partial-vector [link-vector output]
  (with-job-conf {"mapred.job.priority" "VERY_LOW"}
    (let [source (<- [?date ?link ?count] ((hfs-textline link-vector) ?line) (o/extract 3  #"[^\t]+" ?line :> ?date ?link ?count))
	  word-vec (<-  [?date-?word ?count] (source ?date ?link ?link-count)
	   (get-article-content ?link :> ?content)
	   (o/split ?content :> ?word)
	   (cs/lower-case ?word :> ?word)
	   (cs/replace ?word #"[^a-z\-\_]" "" :> ?word)
	   (str ?date "|" ?word :> ?date-word) ; Again concatenated so that the count is per day
	   (no-repeats ?word)
	   (valid-word ?word)
	   (valid-length ?word)
	   (c/count ?word-count)
	   (* ?word-count ?link-count :> ?count)
	   )]
      (?<- "Create-partial-word-vector-from-news-links" (hfs-textline output) [?date ?word ?count] (word-vec ?date-word ?count) 
			(o/extract 2 #"[^\|]+" ?date-word :> ?date ?word) 
			(:distinct true))
      )
    )
  )
	    
(defn build-tweet-news-vector [tweet-loc word-vector-loc]
	(println (str "**Building tweet news word vectors into " word-vector-loc " from " tweet-loc))
	(workflow ["/tmp/max/twitter_news_vector_tmp"]
		stage-1 ([:tmp-dirs link-vector-step]
			(create-link-vector tweet-loc link-vector-step)
			)
		stage-2 ([:deps stage-1 :tmp-dirs partial-tweet-vector-step]
			(create-word-partial-vector link-vector-step partial-tweet-vector-step)
			)
		stage-3 ([:tmp-dirs date-word-sums]
			(wv/compute-date-word-total partial-tweet-vector-step date-word-sums) ;At some point this can be replaced with a ??<-
			)
		stage-4 ([:deps [stage-1 stage-2]]
			(wv/combine-into-true-word-vector partial-tweet-vector-step date-word-sums word-vector-loc)
			)		
	)
)


(defn -main [tweet-loc friendlyname]
  (println (str "**Running news vector workflow on " friendlyname ))
  (let [word-vector-loc (str "/user/maxdupenois/twitter-news-word-vectors/dated=" friendlyname)]
    (let [twitter-links (<- [?date ?word] ((su/globhfs-textfile tweet-loc) ?line) (o/extract 8 #"[^\t]+" ?line  :> ?year ?month ?day _ _ _ ?tweet ?timezone)
			        (valid-tweet ?timezone)
				(make-date ?year ?month ?day :> ?dt)
				(fix-date ?dt :> ?date)
				(o/split ?tweet :> ?word)
				(cs/lower-case ?word :> ?word)
				(is-link? ?word))]
      (?<- (stdout) [?date-link ?count] (twitter-links ?date ?tlink) (true-url ?tlink :> ?link) (str ?date "|" ?link :> ?date-link) (c/count ?count))
      )
    ;; (?<- (stdout) [?date ?link ?count] ((su/globhfs-textfile tweet-loc) ?line) (o/extract 8 #"[^\t]+" ?line  :> ?year ?month ?day _ _ _ ?tweet ?timezone)
    ;; 			        (valid-tweet ?timezone)
    ;; 				(make-date ?year ?month ?day :> ?dt)
    ;; 				(fix-date ?dt :> ?date)
    ;; 				(o/split ?tweet :> ?word)
    ;; 				(cs/lower-case ?word :> ?word)
    ;; 				(is-link? ?word)
    ;; 				;(identity ?word :> ?link)
    ;; 				(true-url ?word :> ?link)
    ;; 				;(str ?date "|" ?link :> ?date-link) ;Combined so that the count is per-day
    ;; 				(c/count ?count))
    
    ;; (if  (not (su/hdp-path-exists? word-vector-loc)) 
    ;;   (build-tweet-news-vector tweet-loc word-vector-loc)
    ;;   )
    ;; (?<- (stdout) [?date] ((hfs-textline (str "/user/maxdupenois/twitter-similarities/tmp/true-word-vec/dated=" friendlyname)) ?line) (o/extract 5 #"[^\t]+" ?line :> ?word ?dt ?norm) (fix-date ?dt :> ?date) (:distinct true))
    

    ; (s/build-similarities-keep-stages word-vector-loc friendlyname))
    )
  )
  
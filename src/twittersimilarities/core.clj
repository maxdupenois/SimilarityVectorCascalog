(ns twittersimilarities.core
	(:use cascalog.api
	cascalog.checkpoint)
	(:require [twittersimilarities.wordvectors :as wv]
		[twittersimilarities.similarities :as s]
		[twittersimilarities.readers :as r]
		[twittersimilarities.utils :as su]
		[twittersimilarities.ops :as o])
   (:gen-class))
	






;; (defn fix-date [date]
;; (let [date-parts (seq (.split date "\\-") )
;; 	year (first date-parts)
;; 	month (Integer/parseInt (second date-parts))
;; 	day (Integer/parseInt (nth date-parts 2))
;; 	]
;;     (println date-parts)
;;     (str year "-" (if (< month 10) (identity "0") (identity "")) month "-" (if (< day 10) (identity "0") (identity "")) day)
;;        )
;;   )


(defn -main [tweet-loc friendlyname]
  (println (str "**Running tweet similarity workflow on " friendlyname ))
  (let [
	word-vector-loc (str "/user/maxdupenois/twitter-word-vectors/dated=" friendlyname)]
    (if  (not (su/hdp-path-exists? word-vector-loc)) 
      (wv/build-tweet-vector tweet-loc word-vector-loc)
      )
    ;; (?<- (stdout) [?date] ((hfs-textline (str "/user/maxdupenois/twitter-similarities/tmp/true-word-vec/dated=" friendlyname)) ?line) (o/extract 5 #"[^\t]+" ?line :> ?word ?dt ?norm) (fix-date ?dt :> ?date) (:distinct true))
    

    (s/build-similarities-keep-stages word-vector-loc friendlyname))
 )
  
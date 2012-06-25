(ns twittersimilarities.readers
   (:use cascalog.api)
   (:require [twittersimilarities.ops :as o])
   (:gen-class))

(defn read-from-totals [location]
	(with-job-conf {"mapred.job.priority" "VERY_LOW"}
	(let [src (hfs-textline location)]
		(<- [?date ?total] (src ?line) (o/extract 2 #"[^\t]+" ?line :> ?date ?total) (:distinct true))
	))
)

(defn read-from-partial-word-vec [location]
	(with-job-conf {"mapred.job.priority" "VERY_LOW"}
	(let [src (hfs-textline location)]
		(<- [?date ?word ?count] (src ?line) (o/extract 3 #"[^\t]+" ?line :> ?date ?word ?count)  (:distinct true))
	))
)

(defn read-from-word-vec [pth]
	(with-job-conf {"mapred.job.priority" "VERY_LOW"}
	(let [src (hfs-textline pth)]
		(<- [?date ?word ?count ?norm ?sum] (src ?line) 
		(o/extract 5 #"[^\t]+" ?line :> ?date ?word ?count ?norm ?sum)  (:distinct true))
	))
)
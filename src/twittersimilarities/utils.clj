(ns twittersimilarities.utils
   (:use cascalog.api)
   (:require 
	[hadoop-util.core :as hdp]
	)
   (:gen-class))

; ******** Method Defs ********
(defn split-tweet-loc  [tweet-loc]
	 (seq (.split tweet-loc ","))
)

(defn globhfs-textfile [pattern]
	(hfs-textline "" :source-pattern (str "/user/hive/warehouse/twitter_moods/dated=" pattern "/mood*"))
)

(defn hdp-path-exists? [pth]
	(hdp/path-exists? (hdp/filesystem) (hdp/path pth))
)

(defn to-double-fn [x]
	(Double/parseDouble x)
	)

(defn to-int-fn [x]
	(Integer/parseInt x)
	)
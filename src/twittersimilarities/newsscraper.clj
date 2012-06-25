(ns twittersimilarities.newsscraper
   (:require 
	[net.cgrand.enlive-html :as html]
	)
   (:gen-class))

(defn get-true-url [url]
  (if (nil? (re-find #"^http:\/\/.*$" url))
    (identity nil)   
  (let [ conn (cast java.net.HttpURLConnection  (.openConnection (java.net.URL. url)))]
    (try
    (.setInstanceFollowRedirects conn false)
    (.getHeaderField conn "Location")
    (finally (.disconnect conn)))
  )))

(defn fetch-url [url]
  ;; (println (.getHeaderFields (.openConnection (java.net.URL. url))))
  (html/html-resource (java.net.URL. url)))

(defn get-text[node tags]
    (map html/text (html/select node tags)))

(defn bbc-news [node]
  {:headline (get-text node [:h1.story-header] )
   :text    (get-text node  [:div.story-body :p]) }
  )

(defn reuters [node]
  
  {:headline (get-text node [:h1] )
   :text    (get-text node  [:div.column2 :p]) }
  )

(defn cnn [node]
  
  {:headline (get-text node [:div.cnn_storyarea :h1] )
   :text    (get-text node  [:div.cnn_storyarea :p]) }
  )

(defn is-bbc? [url]
  (re-find #"^(http:\/\/(www\.)?bbc\.co\.uk\/news\/)|(http:\/\/news\.bbc\.co\.uk\/).*" url)
  )

(defn is-cnn? [url]
  (re-find #"^http:\/\/.*cnn\.com.*$" url)
  )

(defn is-reuters? [url]
  (re-find #"http:\/\/.*reuters\.com.*$" url)
  )
(defn is-tco? [url]
  (re-find #"^http:\/\/t\.co\/\w*$" url)
  )

(defn get-article[url]
  (let [true-url (if (is-tco? url) (get-true-url url) (identity url))]
    (if (is-bbc? true-url)
      (bbc-news (fetch-url true-url))
      (if (is-cnn? true-url)
	(cnn (fetch-url true-url))
	(if (is-reuters? true-url)
	  (reuters (fetch-url true-url))
	  (identity nil)))))
  )

;; (defn compress [text]
;;    (cs/join " " text)
;;   )



;; (defn -main []

;;   ;; (println (get-article "http://t.co/y0mWjq9l"))
;;   (let [article (get-article  "http://t.co/y0mWjq9l")]
;;     ;; (println (:text article))
;;   )
  
;;   ;; (println (fetch-url "http://t.co/y0mWjq9l")) ;;http://www.bbc.co.uk/news/uk-politics-18531008
;;   ;; (println (cnn (fetch-url "http://edition.cnn.com/2012/06/20/world/meast/syria-us-soldier/index.html?hpt=hp_c2")))
;;   )
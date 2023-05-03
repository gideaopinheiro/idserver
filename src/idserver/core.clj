(ns idserver.core
  (:require [cheshire.core :as json]
            [clj-ulid :as ulid])
  (:gen-class))

(def msg-id (atom 1))

(defn gen-msg-id []
  (swap! msg-id inc))

(defn gen-init-reply
  [msg]
  {:src (:dest msg)
   :dest (:src msg)
   :body {:type "init_ok"
          :msg_id (gen-msg-id)
          :node_id (get-in msg [:body :node_id])
          :node_ids (get-in msg [:body :node_ids])
          :in_reply_to (get-in msg [:body :msg_id])}})

(defn gen-id-reply
  [msg]
  {:src (:dest msg)
   :dest (:src msg)
   :body {:type "generate_ok"
          :id (ulid/ulid)
          :msg_id (get-in msg [:body :msg_id])
          :in_reply_to (get-in msg [:body :msg_id])}})

(defn input-loop
  []
  (doseq [line (line-seq (java.io.BufferedReader. *in*))]
    (if (empty? line)
      nil
      (let [nline (json/decode line true)
            body-type (get-in nline [:body :type])
            node-id (get-in nline [:body :node_id])]
        (binding [*out* *err*]
          (println (str "Received " nline)))
        (cond
          (= body-type "init")
          (do
            (binding [*out* *err*]
              (println (str "Initialized node " node-id)))
            (println (json/encode (gen-init-reply nline))))
          (= body-type "generate")
          (do
            (binding [*out* *err*]
              (println (str "Generate UniqueID " (:body nline))))
            (println (json/encode (gen-id-reply nline)))))))))

(comment
  (gen-id-reply
   {:src "",
    :dest "",
    :body {:type "generate"
           :msg_id 1
           :node_id "n2"
           :node_ids ["n2"]}})

  (gen-init-reply
   {:src "n1",
    :dest "n4",
    :body {:type "init"
           :msg_id 1
           :node_id "n1"
           :node_ids ["n1"]}})
  (input-loop))

(defn -main
  [& args]
  (input-loop))

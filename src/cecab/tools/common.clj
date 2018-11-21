(ns cecab.tools.common
  (:require [cognitect.transit :as transit]
            [datomic.api :as d]))

(import [java.io ByteArrayInputStream ByteArrayOutputStream])


(defn decode-transit
  "Decode from transit format."
  [in]
  (let [reader (transit/reader in :json)]
    (transit/read reader)))

(defn encode-transit
  "Encode clj-value as transit format."
  [clj-value]
  (let [out (ByteArrayOutputStream. 4096)
        writer (transit/writer out :json)
        _ (transit/write writer clj-value)]
    (.toString out)))

(defn get-datoms-from-tx
  "Collect the datoms from a tx taking the history from db"
  [db tx]
  (d/q '[:find ?e ?attr ?v ?added
          :in $ ?tx
          :where
          [?e ?a ?v ?tx ?added]
          [?a :db/ident ?attr ]
          [?tx :db/txInstant ?cuando]]
        (d/history db)
        tx))
;; Taken from gist
;; https://gist.github.com/favila/62276cdb479060b782158e808e1113aa
(defn tx-ids
  "Return a list of transaction ids visible in the given database.
  This is a work-alike to the tx-ids special function inside a
  query and to tx-range, except it uses a database instead of
  a log."
  [db]
  (let [tx-part (d/entid db :db.part/tx)
        ;; NOTE: there are magic "bootstrap" transactions below
        ;; t=1000, but tx-log never shows them. We skip them too
        ;; just to be compatible.
        start-t (d/entid-at db tx-part 1000)]
    (sequence
      (comp
        ;; This is just paranoia. Conceivably it is possible to
        ;; assert :db/txRange on an arbitrary tx-partitioned
        ;; entity number, but that entity is not actually
        ;; a transaction t?
        (filter (fn [{:keys [e tx]}] (= e tx)))
        (map :e)
        (take-while #(= (d/part %) tx-part)))
      (d/seek-datoms db :aevt :db/txInstant start-t))))

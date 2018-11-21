(ns cecab.tools.boa
  (:require [org.httpkit.client :as http]
            [datomic.api :as d]
            [cecab.tools.common :as common]))

(defn post-request
  "Post a Request to API/Gateway identified by url with
   a content value, which is a CLJ value"
  [url value]
  (let [
        {:keys [status headers body error] :as resp}
        @(http/post
          url
          {:body
           (str value)})
        clj-content (common/decode-transit body)]
    clj-content))


(defn get-txs-for-schema
    "Get a vector sorted by tx ID for all the transactions
     that created any custom attribute in db."
    [db]
    (let [epoc  #inst "1970-01-01T00:00:00.000-00:00"]
      (sort
       (d/q '[:find [?tx ...]
               :in $ ?ref
               :where
               [?e :db/valueType ?v ?tx ?added]
               [?e :db/ident ?a ?tx ?added]
               [?tx :db/txInstant ?when]
               [(> ?when ?ref)]]
             (d/history db)
             epoc))))

(defn get-first-custom-tx-schema
    "Find the first transaction for the schema in db"
    [db]
    (:db/txInstant
     (d/pull db '[:db/txInstant]
              (-> db get-txs-for-schema first))))


(defn get-schema-attribs
    "Get a list of all attributes in pro-db taken from the history 
     of the database"
    [pro-db]
    (d/q '[:find [?v ...]
            :in $ [?tx ...]
            :where
            [?e :db/ident ?v ?tx ?added]]
          (d/history pro-db)
          (get-txs-for-schema pro-db)))

(defn get-all-tx-data
    "It give a list of all the transactions excepto those
     that created attributes in the schema of pro-db"
    [pro-db]
    (sort
     (d/q '[:find [?tx ...]
             :in $ ?ref  [?a ...]
             :where
             [?e ?a ?v ?tx ?added]
             [?tx :db/txInstant ?when]
             [(>= ?when ?ref)]]
           (d/history pro-db)
           (get-first-custom-tx-schema pro-db)
           (get-schema-attribs pro-db))))

(defn get-map-datatypes
  "Extract the idents for numerical entities that are 
     referenced in the tuples of los-datoms"
  [pro-db los-datoms]
  (reduce
   (fn [acc [x-eid x-attr eid added?]]
     (if (int? eid)
       (if-let [i (:db/ident
                   (d/pull pro-db [:db/ident] eid))]
         (assoc acc eid i)
         acc)
       acc))
   {}
   los-datoms))


(defn fix-datom-values
  "Apply a modificaction to the value of the datom, changing
     keywords like ::a/b"
  [the-datoms]
  (mapv
   (fn [[eid att valor add]]
     (let [new-val (cond
                     (and  (keyword? valor)
                           (not (nil? (namespace valor))))
                     (keyword (-> (namespace valor)
                                  (clojure.string/replace ":" "")) (name valor))
                     :else
                     valor)]
       [eid att new-val add]))
   the-datoms))


(defn load-config
  "Load the config as CLJ value"
  []
  (read-string (slurp "resources/boa/config-boa.edn")))


(defn migrate-db
  "Migrate the db-name into Cloud as specified by config/file"
  [db-name]
  (let [config (load-config)
        ;; Initial changes will happen in this time. Like the
        ;; particular attributes for the migration process.
        migration-date #inst "1970-01-02T00:00:00.000-00:00"
        post-value {:db-name db-name :migration-date migration-date}
        url-apigateway-init-db
        (-> config (get-in [:api-gateways :init-db]))
        create-db? (post-request url-apigateway-init-db post-value)
        pro-uri (-> config (get-in [:on-premise :uri]))
        pro-db 
        (d/db (d/connect pro-uri))
        tx-attribs (get-txs-for-schema pro-db)
        tx-data (get-all-tx-data pro-db)
        all-txs (sort  (concat tx-attribs tx-data))
        url-apigateway-apply-tx
        (-> config (get-in [:api-gateways :apply-tx]))]
    (doall
     (map
      (fn [m-tx ]
        (let [the-datoms (-> (common/get-datoms-from-tx pro-db  m-tx)
                             fix-datom-values)
              mig-data
              {:tx-datoms the-datoms
               :map-datatypes (get-map-datatypes pro-db the-datoms)
               :db-name db-name}]
          (post-request url-apigateway-apply-tx mig-data)))
      all-txs))))

(comment
  (def ensayo-uri "datomic:dev://localhost:4334/ensayo")
  (def db-ensayo (d/db))
  ;; -- Mejor formas de obtener la lista de txs
  ;; https://gist.github.com/favila/62276cdb479060b782158e808e1113aa
  (def doble (map
                  (fn [x]
                    (println x)
                    (* 2 x))))
  (def filtro (filter #(> % 30)))
  (def xtr-all
    (comp filtro doble))
  
  (def lazy-seq (sequence
                 xtr-all
                 (range 0 100)))
  
  (take 3 lazy-seq)
  (62 64 66)

  )

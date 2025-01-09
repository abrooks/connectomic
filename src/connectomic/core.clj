(ns connectomic.core
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [datomic.client.api :as d]
            [frigit.core :as fc]
            [frigit.dump-metadata :as dm])
  (:import [java.util Date]))

(def ^:dynamic *client* (atom nil))
(def ^:dynamic *conn* (atom nil))

(def schema
 [{:db/ident :git/id
   :db/valueType :db.type/string
   :db/cardinality :db.cardinality/one
   :db/unique :db.unique/identity
   :db/doc "The SHA id of a git object"}
  {:db/ident :git/type
   :db/valueType :db.type/keyword
   :db/cardinality :db.cardinality/one
   :db/doc "The type of a git object - :git/commit :git/tag :git/tree :git/blob :git/person"}

  {:db/ident :git.commit/tree 
   :db/valueType :db.type/ref ; -> :git/tree
   :db/cardinality :db.cardinality/one
   :db/doc "Ref to :git/tree of a commit"}
  {:db/ident :git.commit/parent
   :db/valueType :db.type/ref ; -> :git/commit
   :db/cardinality :db.cardinality/many
   :db/doc "Refs to :git/commit entities of parents"}
  {:db/ident :git.commit/author
   :db/valueType :db.type/ref ; -> :git/person
   :db/cardinality :db.cardinality/one
   :db/doc "Ref to the author of a commit"}
  {:db/ident :git.commit/committer
   :db/valueType :db.type/ref ; -> :git/person
   :db/cardinality :db.cardinality/one
   :db/doc "Ref to the committer of a commit"}
  {:db/ident :git.commit/message
   :db/valueType :db.type/string
   :db/cardinality :db.cardinality/one
   :db/doc "The commit message of a commit"}
  {:db/ident :git.commit/author-time
   :db/valueType :db.type/instant
   :db/cardinality :db.cardinality/one
   :db/doc "The author time of a commit"}
  {:db/ident :git.commit/commit-time
   :db/valueType :db.type/instant
   :db/cardinality :db.cardinality/one
   :db/doc "The commit time of a commit"}
  {:db/ident :git.commit/author-tz
   :db/valueType :db.type/string
   :db/cardinality :db.cardinality/one
   :db/doc "The author time of a commit"}
  {:db/ident :git.commit/commit-tz
   :db/valueType :db.type/string
   :db/cardinality :db.cardinality/one
   :db/doc "The commit time of a commit"}

  {:db/ident :git/email
   :db/valueType :db.type/string
   :db/cardinality :db.cardinality/one
   :db/unique :db.unique/identity
   :db/doc "A git person's email"}
  {:db/ident :git/name
   :db/valueType :db.type/string
   :db/cardinality :db.cardinality/many
   :db/doc "A git person's email"}

  {:db/ident :git/repo
   :db/valueType :db.type/ref
   :db/cardinality :db.cardinality/many
   :db/doc "A containing git repository"}

  {:db/ident :filesystem/path
   :db/valueType :db.type/string
   :db/unique :db.unique/identity
   :db/cardinality :db.cardinality/one
   :db/doc "The path of a local git repository"}

  {:db/ident :git.tag/tagger
   :db/valueType :db.type/ref
   :db/cardinality :db.cardinality/one
   :db/doc "Ref to the creator of a tag"}
])

(defn init-db
  []
  (reset! *client* (d/client {:server-type :datomic-local :storage-dir :mem :system "mem"}))
  (d/create-database @*client* {:db-name "mem"})
  (reset! *conn* (d/connect @*client* {:db-name "mem"}))
  (d/transact @*conn* {:tx-data schema}))

(def max-datomic-string-size 4096)

(defn get-repo-origin [path]
  (-> (shell/sh "git" "-C" path "remote" "get-url" "origin")
      :out
      .trim))
  
(def repo-origin (memoize get-repo-origin))

(defn format-commit-txs [objs]
  (let [commits (filter #(-> % second :otype (= :obj_commit)) objs)]
    (mapv (fn [obj]
             (let [[sha {:keys [data]}] obj
                   {:keys [tree parents message author committer]} data
                   local-repo (str/replace (repo-origin (:repo-path (meta obj))) #".*github.com/([^.]+).git" "$1")]
               ;(when (> (count message) 4094) (println "Message too big:\n---\n" message "\n---\n"))
               {:git/id sha
                :git/type :git/commit
                :git/repo {:git/type :git/repo :filesystem/path local-repo}
                :git.commit/tree {:git/id tree :git/type :git/tree}
                :git.commit/parent (mapv (fn [p] {:git/id p :git/type :git/commit}) parents)
                :git.commit/author {:git/email (:email author) :git/name (:name author) :git/type :git/person}
                :git.commit/committer {:git/email (:email committer) :git/name (:name committer) :git/type :git/person}
                :git.commit/author-time (Date. (Long/parseLong (:epochtime author())))
                :git.commit/commit-time (Date. (Long/parseLong (:epochtime committer())))
                :git.commit/author-tz (:tz author)
                :git.commit/commit-tz (:tz committer)
                :git.commit/message (.substring message 0 (min (count message) max-datomic-string-size))}))
          commits)))

(comment
  (require '[snakeskin.alpha :as yaml :refer [convert-node compose]])
  (import '[java.io FileInputStream])
  (require '[connectomic.core :refer :all])
  (init-db)
  (import-commits "/Users/abrooks/repos/personal/frigit/.git/")

  (remove #(-> % first :db/ident str (.startsWith ":db"))
          (d/q '[:find (pull ?p [:db/ident :db/valueType :db/cardinality :db/unique])
                 :where [?p :db/ident]]
             (d/db @*conn*)))

  (time (clojure.pprint/pprint (d/q '[:find ?email (pull ?e [:git/name]) :where [?e :git/type :git/person][?e :git/name ?name][?e :git/email ?email][?e :git/name ?nameb][(not= ?name ?nameb)]] (d/db @*conn*))))

  (->> "/Users/abrooks/repos/foss/conlink/test/utils-compose.yaml" (FileInputStream.) (.composeAllFromInputStream compose) (map convert-node) clojure.pprint/pprint)
)

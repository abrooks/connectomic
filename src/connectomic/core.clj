(ns connectomic.core
  (:require [clojure.core.async :as a :refer [>!! <! >! go-loop]]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [datomic.client.api :as d]
            [frigit.core :as fc]
            [frigit.dump-metadata :as dm])
  (:import [java.io File]
           [java.util Date]))

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
   {:db/ident :git.commit/message-size
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "The size of a commit message of a commit"}
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

   {:db/ident :git.tree/entry
    :db/valueType :db.type/ref ; -> :git.tree/entry
    :db/cardinality :db.cardinality/many
    :db/isComponent true
    :db/doc "The entry in tree node"}
   {:db/ident :git.tree.entry/mode
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "The POSIX permission mode of this git tree entry"}
   {:db/ident :git.tree.entry/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "The name (path element) of this git tree entry"}
   {:db/ident :git.tree.entry/obj
    :db/valueType :db.type/ref ; -> :git/tree or :git/blob
    :db/cardinality :db.cardinality/one
    :db/doc "Object (tree/blob) pointed to by this git tree entry"}

  ;; :git/blob is just a :git/type, not an attribute

   {:db/ident :git/email
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/doc "A git person's email"}
   {:db/ident :git/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many
    :db/doc "A git person's name"}

   ;; a repo has:
   ;; - a filesystem/path * unique
   ;; - a remote
   ;; - refs (component)
   ;; - tag refs (component)
   ;;
   ;; a ref has
   ;; a name
   ;; a sha
   ;;
   ;; a tag ref has
   ;; a ref
   ;; a sha pointer often to an annotated tag object
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
   {:db/ident :git.tag/tag-time
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/doc "Ref to the creator of a tag"}
   {:db/ident :git.tag/tag-tz
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "Ref to the creator of a tag"}])

(def db-name "primary")

(defn init-db
  []
  (reset! *client* (d/client {:server-type :datomic-local
                              :system "connectomic"}))
  (d/create-database @*client* {:db-name @#'db-name})
  (reset! *conn* (d/connect @*client* {:db-name @#'db-name}))
  (d/transact @*conn* {:tx-data schema}))

(defn delete-db
  []
  (reset! *client* (d/client {:server-type :datomic-local :system "connectomic"}))
  (d/delete-database @*client* {:db-name @#'db-name}))

(def max-datomic-string-size 4096)

(defn get-repo-origin [path]
  (-> (shell/sh "git" "-C" path "remote" "get-url" "origin")
      :out
      .trim))

(def repo-origin (memoize get-repo-origin))

(defn format-commit-txs [objs]
  (map (fn [[sha {:keys [data type]} :as obj]]
         (case type
           :obj_commit
           (let [{:keys [tree parents message author committer]} data
                 local-repo (str/replace (repo-origin (:repo-path obj)) #".*github.com/([^.]+).git" "$1")]
               ;(when (> (count message) 4094) (println "Message too big:\n---\n" message "\n---\n"))
             {:git/id sha
              :git/type :git/commit
              ;; TODO Remove from commit
              :git/repo {:git/type :git/repo :filesystem/path local-repo}
              :git.commit/tree {:git/id tree :git/type :git/tree}
              :git.commit/parent (mapv (fn [p] {:git/id p :git/type :git/commit}) parents)
              :git.commit/author {:git/email (:email author) :git/name (:name author) :git/type :git/person}
              :git.commit/committer {:git/email (:email committer) :git/name (:name committer) :git/type :git/person}
              :git.commit/author-time (Date. (* 1000 (Long/parseLong (:epochtime author ()))))
              :git.commit/commit-time (Date. (* 1000 (Long/parseLong (:epochtime committer ()))))
              :git.commit/author-tz (:tz author)
              :git.commit/commit-tz (:tz committer)
              :git.commit/message-size (count message)
              :git.commit/message (.substring message 0 (min (count message) max-datomic-string-size))})
           :obj_tree
           {:git/id sha
            :git/type :git/tree
            :git.tree/entry (mapv (fn [[mode name obj_sha]]
                                    {:git.tree.entry/mode mode
                                     :git.tree.entry/name name
                                     :git.tree.entry/obj {:git/id obj_sha}})
                                  data)}

           :obj_blob
           {:git/id sha :git/type :git/blob}
           :obj_tag
           {:git/id sha :git/type :git/tag}
             ;; Default
           (do (prn :unknown-type type)
               {:git/type :git/unknown})))
       objs))

(defn get-known-shas [db]
  (into #{} (map first (d/q '[:find ?id :where [_ :git/id ?id]] db))))

(defn gen-commit-filter [known-shas]
  (fn [[sha :as obj]]
    (when (known-shas sha)
      obj)))

(defn dump-subdirs [path]
  (let [dirs (->> path File. .listFiles (map #(str % "/.git")))
        dir-count (count dirs)]
    (pmap (fn [[i d]]
            (do
              (println (format ">> [%d/%d] %s" (inc i) dir-count d))
              (let [r (fc/walk-git-db dm/dump-metadata d)]
                (println (format "<< [%d/%d] %s" (inc i) dir-count d))
                r)))
          (map list (range) (sort dirs)))))

(defn ingest-new-commits [path]
  (time (doseq [t (->> (dm/dump-subdirs path)
                       (apply concat)
                       (remove @(future (-> (d/db @*conn*) get-known-shas gen-commit-filter)))
                       format-commit-txs
                       (partition-all 100))]
          (time (datomic.client.api/transact @*conn* {:tx-data t})))))

(defn dump-refs [path]
  (let [dirs (->> path File. .listFiles (map #(str % "/.git")))
        dir-count (count dirs)]
    (pmap (fn [[i d]]
            (do
              (println (format ">> [%d/%d] %s" (inc i) dir-count d))
              (let [r (fc/walk-git-db dm/dump-metadata d)]
                (println (format "<< [%d/%d] %s" (inc i) dir-count d))
                r)))
          (map list (range) (sort dirs)))))

#_
(defn ingest-refs [path]
  (time (doseq [t (->> (dm/dump-refs path)
                       (apply concat)
                       format-ref-txs
                       (partition-all 100))]
          (time (datomic.client.api/transact @*conn* {:tx-data t})))))

(comment
  (require '[snakeskin.alpha :as yaml :refer [convert-node compose]])
  (import '[java.io FileInputStream])
  (require '[connectomic.core :refer :all])
  (init-db)

  (remove #(-> % first :db/ident str (.startsWith ":db"))
          (d/q '[:find (pull ?p [:db/ident :db/valueType :db/cardinality :db/unique])
                 :where [?p :db/ident]]
               (d/db @*conn*)))
  (transient {})
  (def repo-dir "/path/to/repos")
  (time (doseq [t (partition-all 100 (format-commit-txs (remove @(future (-> (d/db @*conn*) get-known-shas gen-commit-filter)) (apply concat (dm/dump-subdirs repo-dir)))))] (time (datomic.client.api/transact @*conn* {:tx-data t}))))

  (time (loop [pass 0] (let [c (count (d/datoms (d/db @*conn*) {:index :avet :components [:git/id] :start (* pass 1000)}))] (if (= c 1000) (recur (inc pass)) (+ c (* 1000 pass))))))

  (->> (d/db @*conn*)
       (d/qseq '[:find ?id ?n ?t
                 :where
                 [?p :git/email ?id]
                 (not (or [(.endsWith ?id ".com")]
                          [(.endsWith ?id ".local")]))
                 [?c :git.commit/author ?p]
                 [?c :git.commit/author-time ?t]
                 [?p :git/name ?n]])
       (map (fn [[e n t]] [e n (+ 1900 (.getYear t))]))
       (into #{})
       (group-by first)
       clojure.pprint/pprint)

  (->> "/Users/abrooks/repos/foss/conlink/test/utils-compose.yaml" (FileInputStream.) (.composeAllFromInputStream compose) (map convert-node) clojure.pprint/pprint))

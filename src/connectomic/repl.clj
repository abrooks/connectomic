(ns connectomic.repl
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [connectomic.core :refer [*conn*]]
            [datomic.client.api :as d]
            [frigit.core :as fc]
            [frigit.dump-metadata :as dm])
  (:import [java.io File]
           [java.util Date UUID]))

(def rules
  '[[(below ?p ?c)
     [?e :git.tree.entry/obj ?c]
     [?p :git.tree/entry ?e]]
    [(below ?p ?c)
     (below ?pi ?c)
     [?pe :git.tree.entry/obj ?pi]
     [?p :git.tree/entry ?pe]]
    [(below-path ?p ?c ?path)
     [?e :git.tree.entry/obj ?c]
     [?p :git.tree/entry ?e]]
    [(below-path ?p ?c ?path)
     (below-path ?pi ?c ?path)
     [?pe :git.tree.entry/obj ?pi]
     [?p :git.tree/entry ?pe]]])

(->> (d/db @*conn*)
     (d/qseq '[:find ?id ?n ?t
               :where
               [?p :git/email ?id]
               (not (or [(.endsWith ?id ".com")]
                        [(.endsWith ?id ".local")]))
               [?p :git/name ?n]
               [?c :git.commit/author ?p]
               [?c :git.commit/author-time ?t]])
     (map (fn [[e n t]] [e n (+ 1900 (.getYear t))]))
     (into #{})
     (group-by first))

(->> (d/db @*conn*)
     (d/qseq '[:find ?z ?p
               :where
               [?c :git.commit/author-tz ?z]
               [?c :git.commit/author ?p]])
     (map first)
     frequencies)

(->> (d/db @*conn*)
     (d/qseq '[:find ?n
               :where
               [?t :git.tree.entry/name ?n]
               [(.endsWith ?n ".yaml")]
               [?t :git.tree.entry/obj ?o]
               [?o :git/type :git/blob]])
     (map first)
     (into #{})
     ((juxt count identity)))

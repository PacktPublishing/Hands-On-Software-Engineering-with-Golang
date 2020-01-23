FROM cockroachdb/cockroach

COPY Chapter06/linkgraph/store/cdb/migrations /migrations
COPY Chapter10/cdb-schema/bootstrap-db.sh .

ENTRYPOINT ["bash", "./bootstrap-db.sh"]


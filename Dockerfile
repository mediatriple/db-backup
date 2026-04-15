FROM mysql:8.0.45-debian

WORKDIR /work

COPY db-backup.sh /opt/db-backup/script/db-backup.sh

RUN chmod 0555 /opt/db-backup/script/db-backup.sh \
    && mkdir -p /opt/db-backup/config /work/backups

ENV TZ=Europe/Istanbul

ENTRYPOINT ["/opt/db-backup/script/db-backup.sh"]
CMD ["/opt/db-backup/config/db-backup.conf"]

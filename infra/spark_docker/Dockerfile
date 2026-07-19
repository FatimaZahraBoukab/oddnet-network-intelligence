FROM bitnamilegacy/spark:3.5

USER root

COPY start_master_and_job.sh /start_master_and_job.sh
RUN chmod +x /start_master_and_job.sh

ENTRYPOINT ["/start_master_and_job.sh"]

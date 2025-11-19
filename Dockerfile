FROM harbor.eks-cicd.useast1.master.fullcontact.com/devops/jvmrunner-openjdk8:stable
# https://github.com/docker-library/openjdk/issues/145#issuecomment-334561903
# https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=894979
USER root
RUN mkdir -p /opt/secor
ADD target/secor-*-bin.tar.gz /opt/secor/

COPY src/main/scripts/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

RUN chown -R jvmrunner:jvmrunner /opt/secor /docker-entrypoint.sh

USER jvmrunner

ENTRYPOINT ["/docker-entrypoint.sh"]

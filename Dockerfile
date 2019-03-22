FROM ruby:2.5.3-alpine3.8

ADD . /flood-source
WORKDIR /flood-source
RUN bundle install

ENTRYPOINT ["bundle", "exec", "ruby", "flood.rb"]
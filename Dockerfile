FROM --platform=linux/arm64 ruby:3.4.5

ENV RAILS_ENV=prod
ENV RAILS_LOG_TO_STDOUT=true

# Copy the Gemfile as well as the Gemfile.lock and install gems.
# This is a separate step so the dependencies will be cached.
RUN mkdir medusa-diff
WORKDIR medusa-diff

COPY Gemfile Gemfile.lock  ./
RUN gem install bundler && bundle install

# Copy the main application, except whatever is listed in .dockerignore.
COPY . ./

CMD ["echo", "Error running task, please check the container override command!"]
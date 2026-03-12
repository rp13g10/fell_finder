FROM rust:1.93

# Copy source
COPY packages/fell_finder/ /fell_finder
WORKDIR /fell_finder

# Build binaries
RUN cargo build --release

# Open port to receive API requests
EXPOSE 8000

# Serve the app
CMD ["cargo", "run", "--release"]
# typed: false
# frozen_string_literal: true

# This is a template formula. GoReleaser will automatically update this file
# in the homebrew-tap repository when a new release is published.
#
# To install manually before the tap is set up:
#   brew install --build-from-source ./Formula/scyllamigrate.rb

class Scyllamigrate < Formula
  desc "A lightweight schema migration tool for ScyllaDB"
  homepage "https://github.com/heartwilltell/scyllamigrate"
  license "MIT"

  on_macos do
    on_intel do
      url "https://github.com/heartwilltell/scyllamigrate/releases/download/v0.1.0/scyllamigrate_0.1.0_darwin_x86_64.tar.gz"
      sha256 "PLACEHOLDER_SHA256_DARWIN_AMD64"
    end

    on_arm do
      url "https://github.com/heartwilltell/scyllamigrate/releases/download/v0.1.0/scyllamigrate_0.1.0_darwin_arm64.tar.gz"
      sha256 "PLACEHOLDER_SHA256_DARWIN_ARM64"
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/heartwilltell/scyllamigrate/releases/download/v0.1.0/scyllamigrate_0.1.0_linux_x86_64.tar.gz"
      sha256 "PLACEHOLDER_SHA256_LINUX_AMD64"
    end

    on_arm do
      url "https://github.com/heartwilltell/scyllamigrate/releases/download/v0.1.0/scyllamigrate_0.1.0_linux_arm64.tar.gz"
      sha256 "PLACEHOLDER_SHA256_LINUX_ARM64"
    end
  end

  def install
    bin.install "scyllamigrate"
  end

  test do
    system "#{bin}/scyllamigrate", "-help"
  end
end

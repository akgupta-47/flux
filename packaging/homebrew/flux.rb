class Flux < Formula
  desc "Cross-platform async networking utility"
  homepage "https://github.com/akgupta-47/flux"
  url "https://github.com/akgupta-47/flux.git", tag: "v#{version}", using: :git
  version "0.1.0"
  license "MIT"
  depends_on "rust" => :build

  def install
    system "cargo", "install", "--locked", "--path", ".", "--root", prefix
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/flux --version")
  end
end

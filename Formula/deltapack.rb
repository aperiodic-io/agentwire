# Homebrew: `brew tap aperiodic-io/deltapack https://github.com/aperiodic-io/deltapack`, then
# `brew install deltapack` (a release, once one is tagged: each release attaches this formula with
# its url and sha256) or `brew install --HEAD deltapack` (main, built from source).
class Deltapack < Formula
  desc "OpenTelemetry collector and recording proxy that stores LLM prompts as deltas"
  homepage "https://github.com/aperiodic-io/deltapack"
  license "Apache-2.0"
  head "https://github.com/aperiodic-io/deltapack.git", branch: "main"

  depends_on "rust" => :build

  def install
    system "cargo", "install", *std_cargo_args, "--features", "proxy"
  end

  test do
    assert_match "deltapack", shell_output("#{bin}/deltapack --version")
    (testpath/"in.jsonl").write("{\"turn\":1}\n{\"turn\":2}\n")
    system "sh", "-c", "#{bin}/deltapack log pack #{testpath}/t.dpk < #{testpath}/in.jsonl"
    assert_equal "{\"turn\":1}\n{\"turn\":2}\n", shell_output("#{bin}/deltapack log unpack #{testpath}/t.dpk")
  end
end

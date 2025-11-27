using System.Runtime.InteropServices;
using Xunit;

namespace Albatross.IO.Test {
	public class TestIOUtility {
		[Theory]
		[InlineData("abc-123", " ", "abc-123")]
		[InlineData("abc-123#", "-", "abc-123#")]
		[InlineData("test.txt", "-", "test.txt")]
		[InlineData("abc-123*", "", "abc-123")]
		public void TestConvertText2Filename(string text, string filler, string expected) {
			if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX) || RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) {
				Assert.Equal(text, text.ConvertToFilename(filler));
			} else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) {
				Assert.Equal(expected, text.ConvertToFilename(filler));
			}
		}
	}
}
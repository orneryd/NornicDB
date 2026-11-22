// Capture observed context properties from the pinned JDK, not iterator code.
// Usage: java --add-opens java.base/sun.text=ALL-UNNAMED
//   scripts/SigmaContextSnapshot.java /external/snapshots
// Compress each raw .bin with Python gzip.compress(data, mtime=0).
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.BreakIterator;
import java.util.Locale;

class SigmaContextSnapshot {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("external output directory required");
        Path output = Path.of(args[0]);
        Files.createDirectories(output);
        var iterator = BreakIterator.getWordInstance(Locale.ROOT);
        var lookup = iterator.getClass().getDeclaredMethod("lookupCategory", int.class);
        lookup.setAccessible(true);
        byte[] properties = new byte[0x110000];
        byte[] categories = new byte[properties.length];
        for (int r = 0; r < properties.length; r++) {
            int type = Character.getType(r), bits = 0;
            if (Character.isLetter(r)) bits |= 1;
            if (type == Character.COMBINING_SPACING_MARK) bits |= 2;
            if (type == Character.DECIMAL_DIGIT_NUMBER || type == Character.LETTER_NUMBER || type == Character.OTHER_NUMBER) bits |= 4;
            if (type == Character.NON_SPACING_MARK || type == Character.ENCLOSING_MARK) bits |= 8;
            if (type == Character.FORMAT) bits |= 16;
            if (type == Character.DASH_PUNCTUATION || type == Character.CONNECTOR_PUNCTUATION) bits |= 32;
            if (type == Character.LOWERCASE_LETTER || type == Character.UPPERCASE_LETTER || type == Character.TITLECASE_LETTER) bits |= 64;
            properties[r] = (byte) bits;
            categories[r] = (byte) ((int) lookup.invoke(iterator, r));
        }
        Files.write(output.resolve("java-context.bin"), properties);
        Files.write(output.resolve("java-break-context.bin"), categories);
        System.out.println("Runtime " + Runtime.version());
    }
}

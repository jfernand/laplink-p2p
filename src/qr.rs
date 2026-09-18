use qrcode::{EcLevel, QrCode, Version};

fn qr_to_braille(text: &str, invert: bool) -> String {
    // 1. Generate the standard QR code matrix with a small 1-unit border
    let code = QrCode::with_version(text, Version::Normal(1), EcLevel::L)
        .expect("Failed to generate QR code");

    let width = code.width();
    let mut output = String::new();

    // 2. Iterate through the matrix in chunks of 4 rows and 2 columns
    // Braille patterns map perfectly to a 2x4 bitmask grid
    for y in (0..width).step_by(4) {
        for x in (0..width).step_by(2) {
            let mut char_code = 0u32;

            // Helper closure to safely read the matrix and handle inverted backgrounds
            let get_bit = |nx: usize, ny: usize| -> bool {
                if nx < width && ny < width {
                    // qrcode crate uses Color::Light and Color::Dark
                    let is_dark = code[(nx, ny)] == qrcode::Color::Dark;
                    if invert { !is_dark } else { is_dark }
                } else {
                    // Pad out-of-bounds with background color
                    invert
                }
            };

            // Map the 2x4 pixel grid onto the standard Unicode Braille bit flags
            // Left Column (dots 1, 2, 3, 7)
            if get_bit(x, y)     { char_code |= 0x01; }
            if get_bit(x, y + 1) { char_code |= 0x02; }
            if get_bit(x, y + 2) { char_code |= 0x04; }
            if get_bit(x, y + 3) { char_code |= 0x40; }

            // Right Column (dots 4, 5, 6, 8)
            if get_bit(x + 1, y)     { char_code |= 0x08; }
            if get_bit(x + 1, y + 1) { char_code |= 0x10; }
            if get_bit(x + 1, y + 2) { char_code |= 0x20; }
            if get_bit(x + 1, y + 3) { char_code |= 0x80; }

            // U+2800 is the blank Braille offset base character
            if let Some(ch) = char::from_u32(0x2800 + char_code) {
                output.push(ch);
            }
        }
        output.push('\n');
    }

    output
}

#[cfg(test)]
mod tests {
    use crate::qr::qr_to_braille;

    #[test]
    fn test_qr() {
        let text = qr_to_braille("jfernand@me.com", true);
        println!("{}", text);
    }
}
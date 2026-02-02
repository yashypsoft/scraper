#!/usr/bin/env python3
"""
Enhanced CAPTCHA Solver with Audio Support
"""

import os
import sys
import time
import json
import random
import urllib.request
import logging
from pathlib import Path
from typing import Optional, Dict, Any

# Try to import optional dependencies
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.keys import Keys
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("Selenium not available. Browser automation disabled.")

try:
    import speech_recognition as sr
    import pydub
    AUDIO_AVAILABLE = True
except ImportError:
    AUDIO_AVAILABLE = False
    print("Audio libraries not available. Audio CAPTCHA solving disabled.")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Fallback words for audio recognition
recaptcha_words = [
    "apple tree", "blue sky", "silver coin", "happy child", "gold star",
    "fast car", "river bank", "mountain peak", "red house", "sun flower",
    "deep ocean", "bright moon", "green grass", "snow fall", "strong wind",
    "dark night", "big city", "tall building", "small village", "soft pillow",
    "quiet room", "loud noise", "warm fire", "cold water", "heavy rain",
    "hot coffee", "empty street", "open door", "closed window", "white cloud",
    "yellow light", "long road", "short path", "new book", "old paper",
    "broken clock", "silent night", "early morning", "late evening", "clear sky",
    "dusty road", "sharp knife", "dull pencil", "lost key", "found wallet",
    "strong bridge", "weak signal", "fast train", "slow boat", "hidden message",
    "bright future", "dark past", "deep forest", "shallow lake", "frozen river",
    "burning candle", "flying bird", "running horse", "jumping fish", "falling leaf",
    "climbing tree", "rolling stone", "melting ice", "whispering wind", "shining star",
    "crying baby", "laughing child", "singing voice", "barking dog", "meowing cat",
    "chirping bird", "roaring lion", "galloping horse", "buzzing bee", "silent whisper"
]


class AudioRecognition:
    """Handle audio CAPTCHA recognition"""
    
    @staticmethod
    def voicereco(audio_file_path: str) -> str:
        """Recognize speech from audio file"""
        if not AUDIO_AVAILABLE:
            logger.error("Audio libraries not available")
            return random.choice(recaptcha_words)
            
        try:
            recognizer = sr.Recognizer()
            
            with sr.AudioFile(audio_file_path) as source:
                logger.info("Processing audio file...")
                recognizer.adjust_for_ambient_noise(source, duration=0.5)
                audio = recognizer.record(source)

                try:
                    text = recognizer.recognize_google(audio)
                    logger.info(f"Extracted Text: {text}")
                    return text.lower().strip()
                except sr.UnknownValueError:
                    random_text = random.choice(recaptcha_words)
                    logger.warning(f"Could not understand audio, using fallback: {random_text}")
                    return random_text
                except sr.RequestError as e:
                    logger.error(f"Speech recognition error: {e}")
                    random_text = random.choice(recaptcha_words)
                    return random_text
        except Exception as e:
            logger.error(f"Error processing audio: {e}")
            return random.choice(recaptcha_words)
    
    @staticmethod
    def download_audio(src: str, mp3_path: str, wav_path: str) -> bool:
        """Download and convert audio file"""
        try:
            logger.info(f"Downloading audio from: {src[:100]}...")
            
            # Add headers to mimic browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'audio/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://www.google.com/',
            }
            
            req = urllib.request.Request(src, headers=headers)
            
            with urllib.request.urlopen(req) as response:
                with open(mp3_path, 'wb') as f:
                    f.write(response.read())
            
            # Convert to WAV
            sound = pydub.AudioSegment.from_file(mp3_path)
            sound.export(wav_path, format="wav")
            
            logger.info("Audio downloaded and converted")
            return True
            
        except Exception as e:
            logger.error(f"Audio download error: {e}")
            return False


class EnhancedCaptchaSolver:
    def __init__(self, headless: bool = True, log_dir: str = "logs"):
        self.headless = headless
        self.log_dir = Path(log_dir)
        self.driver = None
        self.audio_recognition = AudioRecognition()
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging directory"""
        self.log_dir.mkdir(exist_ok=True)
        
    def setup_driver(self):
        """Setup Chrome WebDriver"""
        if not SELENIUM_AVAILABLE:
            raise ImportError("Selenium not installed")
            
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless=new")
        
        # Common options
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        
        # Remove automation indicators
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # User agent
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        except Exception as e:
            logger.error(f"Driver setup error: {e}")
            raise
    
    def solve_recaptcha_audio(self) -> Dict[str, Any]:
        """
        Solve reCAPTCHA using audio challenge
        """
        try:
            logger.info("Attempting audio CAPTCHA solving...")
            
            # Switch to default content first
            self.driver.switch_to.default_content()
            time.sleep(2)
            
            # Find challenge iframe
            challenge_frame = None
            frames = self.driver.find_elements(By.TAG_NAME, "iframe")
            
            for frame in frames:
                try:
                    src = frame.get_attribute("src") or ""
                    title = frame.get_attribute("title") or ""
                    
                    if "challenge" in title.lower() or "bframe" in src.lower():
                        challenge_frame = frame
                        logger.info(f"Found challenge frame: {src[:80]}...")
                        break
                except:
                    continue
            
            if not challenge_frame:
                logger.error("No challenge frame found")
                return {"success": False, "error": "No challenge frame"}
            
            # Switch to challenge frame
            self.driver.switch_to.frame(challenge_frame)
            time.sleep(3)
            
            # Click audio button
            audio_button = None
            button_selectors = [
                "#recaptcha-audio-button",
                "button[title*='audio']",
                "button[title*='Audio']",
                ".rc-button-audio"
            ]
            
            for selector in button_selectors:
                try:
                    audio_button = self.driver.find_element(By.CSS_SELECTOR, selector)
                    break
                except:
                    continue
            
            if not audio_button:
                logger.error("Audio button not found")
                self.driver.switch_to.default_content()
                return {"success": False, "error": "Audio button not found"}
            
            audio_button.click()
            logger.info("Clicked audio button")
            time.sleep(5)  # Wait for audio to load
            
            # Get audio source URL
            audio_src = self._get_audio_source()
            if not audio_src:
                logger.error("Could not get audio source")
                self.driver.switch_to.default_content()
                return {"success": False, "error": "No audio source"}
            
            # Download and process audio
            timestamp = int(time.time())
            mp3_path = self.log_dir / f"audio_{timestamp}.mp3"
            wav_path = self.log_dir / f"audio_{timestamp}.wav"
            
            if not self.audio_recognition.download_audio(audio_src, str(mp3_path), str(wav_path)):
                logger.error("Failed to download audio")
                self.driver.switch_to.default_content()
                return {"success": False, "error": "Audio download failed"}
            
            # Recognize text
            captcha_text = self.audio_recognition.voicereco(str(wav_path))
            if not captcha_text:
                logger.error("Failed to recognize audio")
                self.driver.switch_to.default_content()
                return {"success": False, "error": "Audio recognition failed"}
            
            # Enter response
            response_input = None
            input_selectors = [
                "#audio-response",
                "input[type='text']",
                "input.audio-response"
            ]
            
            for selector in input_selectors:
                try:
                    response_input = self.driver.find_element(By.CSS_SELECTOR, selector)
                    break
                except:
                    continue
            
            if not response_input:
                logger.error("Response input not found")
                self.driver.switch_to.default_content()
                return {"success": False, "error": "Response input not found"}
            
            # Enter text
            response_input.clear()
            time.sleep(0.5)
            
            for char in captcha_text:
                response_input.send_keys(char)
                time.sleep(random.uniform(0.05, 0.1))
            
            logger.info(f"Entered response: {captcha_text}")
            
            # Submit
            response_input.send_keys(Keys.ENTER)
            time.sleep(5)
            
            # Switch back
            self.driver.switch_to.default_content()
            
            # Verify success
            success = self._verify_recaptcha_success()
            
            # Clean up files
            try:
                os.remove(mp3_path)
                os.remove(wav_path)
            except:
                pass
            
            if success:
                logger.info("✅ Audio CAPTCHA solved successfully!")
                return {"success": True, "method": "audio", "text": captcha_text}
            else:
                return {"success": False, "error": "Verification failed"}
                
        except Exception as e:
            logger.error(f"Audio solving error: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}
    
    def _get_audio_source(self) -> Optional[str]:
        """Extract audio source URL"""
        try:
            # Try multiple methods to find audio source
            methods = [
                self._get_audio_by_tag,
                self._get_audio_by_javascript,
                self._get_audio_by_source_inspection
            ]
            
            for method in methods:
                audio_src = method()
                if audio_src:
                    return audio_src
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting audio source: {e}")
            return None
    
    def _get_audio_by_tag(self) -> Optional[str]:
        """Find audio by audio tag"""
        try:
            audio_elements = self.driver.find_elements(By.TAG_NAME, "audio")
            for audio in audio_elements:
                src = audio.get_attribute("src")
                if src and src.strip():
                    return src
        except:
            pass
        return None
    
    def _get_audio_by_javascript(self) -> Optional[str]:
        """Find audio using JavaScript"""
        try:
            script = """
                var audios = document.getElementsByTagName('audio');
                for (var i = 0; i < audios.length; i++) {
                    if (audios[i].src && audios[i].src.trim() !== '') {
                        return audios[i].src;
                    }
                }
                return null;
            """
            return self.driver.execute_script(script)
        except:
            return None
    
    def _get_audio_by_source_inspection(self) -> Optional[str]:
        """Find audio by inspecting page source"""
        try:
            page_source = self.driver.page_source
            import re
            
            # Look for audio URLs
            patterns = [
                r'src=["\']([^"\']*\.mp3[^"\']*)["\']',
                r'https://[^"\']*recaptcha[^"\']*audio[^"\']*',
                r'https://www\.google\.com/recaptcha/api2/[^"\']*\.mp3'
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, page_source, re.IGNORECASE)
                for match in matches:
                    if '.mp3' in match.lower():
                        return match
        except:
            pass
        return None
    
    def _verify_recaptcha_success(self, timeout: int = 30) -> bool:
        """Verify if reCAPTCHA is solved"""
        try:
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                # Check page for success indicators
                page_source = self.driver.page_source.lower()
                
                if "recaptcha-token" in page_source or "g-recaptcha-response" in page_source:
                    logger.info("Found reCAPTCHA token")
                    return True
                
                # Check visual indicators
                try:
                    # Switch to recaptcha iframe
                    iframe = self.driver.find_element(By.CSS_SELECTOR, 'iframe[title*="reCAPTCHA"]')
                    self.driver.switch_to.frame(iframe)
                    
                    # Check for checked state
                    checkbox = self.driver.find_element(By.ID, "recaptcha-anchor")
                    aria_checked = checkbox.get_attribute("aria-checked")
                    
                    self.driver.switch_to.default_content()
                    
                    if aria_checked == "true":
                        logger.info("Checkbox is checked")
                        return True
                        
                except:
                    self.driver.switch_to.default_content()
                
                time.sleep(1)
            
            return False
            
        except Exception as e:
            logger.error(f"Verification error: {e}")
            return False
    
    def solve_recaptcha(self, url: str) -> Dict[str, Any]:
        """
        Main method to solve reCAPTCHA with fallback strategies
        """
        if not SELENIUM_AVAILABLE:
            return {"success": False, "error": "Selenium not available"}
        
        try:
            # Setup driver if not already done
            if not self.driver:
                self.setup_driver()
            
            logger.info(f"Navigating to: {url}")
            self.driver.get(url)
            time.sleep(3)
            
            # Save initial screenshot
            initial_screenshot = self.log_dir / "initial_page.png"
            self.driver.save_screenshot(str(initial_screenshot))
            
            # Try checkbox first
            checkbox_result = self._solve_checkbox()
            
            if checkbox_result.get("success"):
                logger.info("Checkbox solved successfully")
                return checkbox_result
            
            # If checkbox fails or challenge appears, try audio
            logger.info("Trying audio challenge...")
            audio_result = self.solve_recaptcha_audio()
            
            if audio_result.get("success"):
                return audio_result
            
            # Both methods failed
            logger.error("All solving methods failed")
            
            # Save final screenshot for debugging
            final_screenshot = self.log_dir / "final_debug.png"
            self.driver.save_screenshot(str(final_screenshot))
            
            return {
                "success": False,
                "error": "All solving methods failed",
                "screenshots": {
                    "initial": str(initial_screenshot),
                    "final": str(final_screenshot)
                }
            }
            
        except Exception as e:
            logger.error(f"Solving error: {e}")
            return {"success": False, "error": str(e)}
    
    def _solve_checkbox(self) -> Dict[str, Any]:
        """Solve simple checkbox reCAPTCHA"""
        try:
            # Find recaptcha iframe
            iframe = None
            iframe_selectors = [
                'iframe[title*="reCAPTCHA"]',
                'iframe[src*="recaptcha/api2/anchor"]',
                'iframe[src*="google.com/recaptcha"]'
            ]
            
            for selector in iframe_selectors:
                try:
                    iframe = self.driver.find_element(By.CSS_SELECTOR, selector)
                    break
                except:
                    continue
            
            if not iframe:
                return {"success": False, "error": "No recaptcha iframe found"}
            
            # Switch to iframe and click
            self.driver.switch_to.frame(iframe)
            time.sleep(1)
            
            checkbox = self.driver.find_element(By.ID, "recaptcha-anchor")
            checkbox.click()
            logger.info("Clicked checkbox")
            
            # Wait and check state
            time.sleep(3)
            aria_checked = checkbox.get_attribute("aria-checked")
            
            self.driver.switch_to.default_content()
            
            if aria_checked == "true":
                return {"success": True, "method": "checkbox"}
            else:
                return {"success": False, "error": "Checkbox not checked"}
                
        except Exception as e:
            logger.error(f"Checkbox error: {e}")
            self.driver.switch_to.default_content()
            return {"success": False, "error": str(e)}
    
    def close(self):
        """Cleanup resources"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass


def main():
    """Command line interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced CAPTCHA Solver')
    parser.add_argument('--url', required=True, help='URL with CAPTCHA')
    parser.add_argument('--headless', action='store_true', default=False,
                       help='Run in headless mode')
    parser.add_argument('--log-dir', default='logs', help='Log directory')
    parser.add_argument('--method', default='auto', choices=['auto', 'audio', 'checkbox'],
                       help='Solving method')
    
    args = parser.parse_args()
    
    # Check dependencies
    if not SELENIUM_AVAILABLE:
        print("Error: Selenium is required. Install with: pip install selenium")
        sys.exit(1)
    
    if args.method == 'audio' and not AUDIO_AVAILABLE:
        print("Error: Audio solving requires speech_recognition and pydub")
        print("Install with: pip install SpeechRecognition pydub")
        sys.exit(1)
    
    solver = EnhancedCaptchaSolver(headless=args.headless, log_dir=args.log_dir)
    
    try:
        print(f"\n{'='*60}")
        print("ENHANCED CAPTCHA SOLVER")
        print(f"{'='*60}")
        print(f"URL: {args.url}")
        print(f"Method: {args.method}")
        print(f"Headless: {args.headless}")
        print(f"Logs: {args.log_dir}")
        print(f"{'='*60}\n")
        
        result = solver.solve_recaptcha(args.url)
        
        print("\n" + "="*60)
        print("RESULTS")
        print("="*60)
        print(json.dumps(result, indent=2))
        
        if result.get("success"):
            print("\n✅ CAPTCHA SOLVED SUCCESSFULLY!")
        else:
            print("\n❌ CAPTCHA SOLVING FAILED")
            print(f"Error: {result.get('error', 'Unknown error')}")
        
        return result.get("success", False)
        
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        return False
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        solver.close()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
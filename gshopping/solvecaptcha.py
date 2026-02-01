import time
import os
import urllib.request
import random
import pydub
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
    "chirping bird", "roaring lion", "galloping horse", "buzzing bee", "silent whisper",
    "drifting boat", "rushing water", "ticking clock", "clicking sound", "typing keyboard",
    "ringing bell", "blinking light", "floating balloon", "spinning wheel", "crashing waves",
    "boiling water", "freezing air", "burning wood", "echoing voice", "howling wind",
    "glowing candle", "rustling leaves", "dancing flame", "rattling chains", "splashing water",
    "twisting road", "swinging door", "glistening snow", "pouring rain", "shaking ground"
]

def voicereco(AUDIO_FILE):
    import speech_recognition as sr

    recognizer = sr.Recognizer()
    
    try:
        with sr.AudioFile(AUDIO_FILE) as source:
            logger.info("🔄 Processing audio file...")
            recognizer.adjust_for_ambient_noise(source, duration=0.5)
            audio = recognizer.record(source)

            try:
                text = recognizer.recognize_google(audio)
                logger.info(f"📝 Extracted Text: {text}")
                return text
            except sr.UnknownValueError:
                random_text = random.choice(recaptcha_words)
                logger.warning(f"❌ Could not understand audio, using fallback: {random_text}")
                return random_text
            except sr.RequestError as e:
                logger.error(f"❌ Speech recognition request error: {e}")
                random_text = random.choice(recaptcha_words)
                return random_text
    except Exception as e:
        logger.error(f"❌ Error processing audio file: {e}")
        random_text = random.choice(recaptcha_words)
        return random_text

def download_audio_file(src, mp3_path, wav_path):
    """Download and convert audio file with retries"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Downloading audio (attempt {attempt + 1}/{max_retries})...")
            urllib.request.urlretrieve(src, mp3_path)
            logger.info("✅ Audio file downloaded.")
            
            # Convert MP3 to WAV
            sound = pydub.AudioSegment.from_mp3(mp3_path)
            sound.export(wav_path, format="wav")
            logger.info("✅ Audio file converted to WAV.")
            return True
        except Exception as e:
            logger.error(f"❌ Audio download/conversion error (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                return False

def wait_for_audio_button(driver):
    """Wait for audio button to be enabled and clickable"""
    max_wait_time = 10
    start_time = time.time()
    
    while time.time() - start_time < max_wait_time:
        try:
            # Try multiple selectors for the audio button
            selectors = [
                (By.ID, "recaptcha-audio-button"),
                (By.XPATH, "//button[contains(@title, 'audio')]"),
                (By.CLASS_NAME, "rc-button-audio"),
                (By.XPATH, "//button[contains(@class, 'rc-button-audio')]"),
            ]
            
            for by, selector in selectors:
                try:
                    audio_button = driver.find_element(by, selector)
                    
                    # Check if button is enabled
                    if audio_button.is_enabled() and "disabled" not in audio_button.get_attribute("class"):
                        logger.info(f"✅ Audio button found and enabled using {by}={selector}")
                        return audio_button
                    else:
                        logger.info("Audio button found but disabled, waiting...")
                        time.sleep(1)
                        continue
                        
                except:
                    continue
                    
        except Exception as e:
            logger.debug(f"Still waiting for audio button: {e}")
            time.sleep(1)
    
    logger.error("❌ Audio button not found or not enabled within timeout")
    return None

def get_audio_source_with_retry(driver):
    """Get audio source URL with retries and multiple strategies"""
    max_retries = 5
    retry_delay = 2
    
    for attempt in range(max_retries):
        logger.info(f"Attempting to get audio source (attempt {attempt + 1}/{max_retries})...")
        
        try:
            # Try multiple selectors for audio source
            selectors = [
                (By.ID, "audio-source"),
                (By.XPATH, "//audio[@id='audio-source']"),
                (By.TAG_NAME, "audio"),
                (By.XPATH, "//*[contains(@src, 'recaptcha')]"),
            ]
            
            audio_source = None
            for by, selector in selectors:
                try:
                    audio_source = driver.find_element(by, selector)
                    if audio_source:
                        logger.info(f"✅ Found audio source using {by}={selector}")
                        break
                except:
                    continue
            
            if not audio_source:
                logger.warning("Audio source not found with standard selectors, trying JavaScript...")
                # Try to find via JavaScript
                audio_elements = driver.execute_script("""
                    return Array.from(document.querySelectorAll('audio')).map(el => ({
                        src: el.src,
                        id: el.id,
                        tagName: el.tagName
                    }));
                """)
                
                if audio_elements and len(audio_elements) > 0:
                    for audio in audio_elements:
                        if 'recaptcha' in audio['src'].lower():
                            logger.info(f"Found audio via JS: {audio['src'][:100]}...")
                            # Create a dummy element reference
                            audio_source = driver.find_element(By.XPATH, f"//audio[@src='{audio['src']}']")
                            break
            
            if audio_source:
                src = audio_source.get_attribute("src")
                if src and src.strip():
                    logger.info(f"✅ Audio source URL obtained: {src[:100]}...")
                    return src
                else:
                    logger.warning("Audio source URL is empty or None")
            
        except Exception as e:
            logger.warning(f"Error getting audio source (attempt {attempt + 1}): {e}")
        
        # Wait before retrying
        if attempt < max_retries - 1:
            logger.info(f"Waiting {retry_delay}s before retry...")
            time.sleep(retry_delay)
            retry_delay *= 1.5  # Exponential backoff
    
    logger.error("❌ Failed to get audio source after all retries")
    return None

def solve_recaptcha_audio(driver):
    """
    Main function to solve reCAPTCHA with improved handling
    """
    try:
        logger.info("Attempting to solve captcha...")
        time.sleep(2)
        
        # Find all iframes
        frames = driver.find_elements(By.TAG_NAME, "iframe")
        logger.info(f"Found {len(frames)} iframes on page")
        
        # Try to find recaptcha frame
        recaptcha_frame = None
        for i, frame in enumerate(frames):
            try:
                src = frame.get_attribute("src") or ""
                title = frame.get_attribute("title") or ""
                if "recaptcha" in src.lower() or "recaptcha" in title.lower():
                    recaptcha_frame = frame
                    logger.info(f"Found recaptcha frame at index {i}: src={src[:50]}..., title={title}")
                    break
            except:
                continue
        
        if not recaptcha_frame:
            logger.info("No recaptcha frame found, might already be solved")
            return "solved"
        
        # Switch to recaptcha frame
        try:
            driver.switch_to.frame(recaptcha_frame)
            logger.info("Switched to recaptcha frame")
        except Exception as e:
            logger.error(f"Failed to switch to recaptcha frame: {e}")
            driver.switch_to.default_content()
            return "quit"
        
        # Click checkbox
        try:
            # Wait for checkbox to be present
            checkbox = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "recaptcha-checkbox-border"))
            )
            
            # Scroll into view
            driver.execute_script("arguments[0].scrollIntoView(true);", checkbox)
            time.sleep(0.5)
            
            # Click using JavaScript to avoid interception
            driver.execute_script("arguments[0].click();", checkbox)
            logger.info("✅ Clicked reCAPTCHA checkbox (via JavaScript)")
            time.sleep(3)
            
        except Exception as e:
            logger.error(f"❌ Error clicking checkbox: {e}")
            driver.switch_to.default_content()
            return "quit"
        
        # Switch back to default content and look for challenge frame
        driver.switch_to.default_content()
        time.sleep(2)
        
        # Look for challenge frame
        challenge_frame = None
        frames = driver.find_elements(By.TAG_NAME, "iframe")
        
        for i, frame in enumerate(frames):
            try:
                src = frame.get_attribute("src") or ""
                title = frame.get_attribute("title") or ""
                if "challenge" in src.lower() or "challenge" in title.lower():
                    challenge_frame = frame
                    logger.info(f"Found challenge frame at index {i}")
                    break
            except:
                continue
        
        if not challenge_frame:
            logger.info("No challenge frame found, CAPTCHA might be solved")
            return "solved"
        
        # Try to solve audio challenge
        max_audio_attempts = 3
        for attempt in range(max_audio_attempts):
            logger.info(f"Audio challenge attempt {attempt + 1}/{max_audio_attempts}")
            
            try:
                # Switch to challenge frame
                driver.switch_to.frame(challenge_frame)
                time.sleep(2)
                
                # Wait for audio button to be enabled
                audio_button = wait_for_audio_button(driver)
                if not audio_button:
                    logger.error("Audio button not available")
                    driver.switch_to.default_content()
                    
                    # Try refreshing the challenge
                    if attempt < max_audio_attempts - 1:
                        logger.info("Refreshing page to get new challenge...")
                        driver.refresh()
                        time.sleep(3)
                        continue
                    else:
                        return "quit"
                
                # Click audio button using JavaScript
                driver.execute_script("arguments[0].click();", audio_button)
                logger.info("✅ Clicked audio challenge button (via JavaScript)")
                time.sleep(3)
                
                # Get audio source with retry
                audio_src = get_audio_source_with_retry(driver)
                if not audio_src:
                    logger.error("Could not get audio source")
                    driver.switch_to.default_content()
                    continue
                
                # Download and process audio
                mp3_path = os.path.join(os.getcwd(), f"captcha_audio_{int(time.time())}.mp3")
                wav_path = os.path.join(os.getcwd(), f"captcha_audio_{int(time.time())}.wav")
                
                if not download_audio_file(audio_src, mp3_path, wav_path):
                    logger.error("Failed to download audio file")
                    driver.switch_to.default_content()
                    continue
                
                # Recognize text
                captcha_text = voicereco(wav_path)
                if not captcha_text:
                    logger.error("Failed to recognize audio")
                    driver.switch_to.default_content()
                    continue
                
                # Enter response
                try:
                    # Find response box
                    response_box_selectors = [
                        (By.ID, "audio-response"),
                        (By.NAME, "audio-response"),
                        (By.XPATH, "//input[@type='text' and contains(@id, 'audio')]"),
                    ]
                    
                    response_box = None
                    for by, selector in response_box_selectors:
                        try:
                            response_box = driver.find_element(by, selector)
                            if response_box:
                                logger.info(f"Found response box using {by}={selector}")
                                break
                        except:
                            continue
                    
                    if not response_box:
                        logger.error("Could not find response box")
                        driver.switch_to.default_content()
                        continue
                    
                    # Clear and enter text
                    response_box.clear()
                    time.sleep(0.5)
                    
                    # Type slowly
                    for ch in captcha_text.lower():
                        response_box.send_keys(ch)
                        time.sleep(random.uniform(0.1, 0.3))
                    
                    # Submit with Enter
                    response_box.send_keys(Keys.ENTER)
                    logger.info(f"✅ Submitted response: {captcha_text}")
                    time.sleep(3)
                    
                    # Switch back and check if solved
                    driver.switch_to.default_content()
                    
                    # Check for verification button
                    try:
                        verify_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Verify')]")
                        if verify_buttons:
                            driver.execute_script("arguments[0].click();", verify_buttons[0])
                            logger.info("Clicked Verify button")
                            time.sleep(2)
                    except:
                        pass
                    
                    # Wait a bit to see if page updates
                    time.sleep(2)
                    
                    # Check if CAPTCHA is gone
                    frames = driver.find_elements(By.TAG_NAME, "iframe")
                    recaptcha_frames = [f for f in frames if "recaptcha" in (f.get_attribute("src") or "").lower()]
                    
                    if len(recaptcha_frames) == 0:
                        logger.info("🎉 CAPTCHA appears to be solved!")
                        return "solved"
                    else:
                        logger.info("CAPTCHA still present, might need another attempt")
                        continue
                    
                except Exception as e:
                    logger.error(f"Error entering response: {e}")
                    driver.switch_to.default_content()
                    continue
                    
            except Exception as e:
                logger.error(f"Error in audio challenge attempt: {e}")
                driver.switch_to.default_content()
                continue
        
        logger.error("❌ Failed to solve CAPTCHA after all attempts")
        return "quit"
        
    except Exception as e:
        logger.error(f"❌ Unexpected error in solve_recaptcha_audio: {e}")
        return "quit"
    finally:
        try:
            driver.switch_to.default_content()
        except:
            pass
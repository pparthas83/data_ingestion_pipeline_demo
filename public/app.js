document.addEventListener('DOMContentLoaded', () => {
  // Elements
  const sliderTech = document.getElementById('slider-tech');
  const sliderSynergy = document.getElementById('slider-synergy');
  const sliderSolve = document.getElementById('slider-solve');
  
  const valTech = document.getElementById('val-tech');
  const valSynergy = document.getElementById('val-synergy');
  const valSolve = document.getElementById('val-solve');
  
  const overallScoreEl = document.getElementById('overall-score');
  const scoreMeter = document.getElementById('score-meter');
  const scoreVerdict = document.getElementById('score-verdict');
  
  const btnApprove30 = document.getElementById('btn-approve-30');
  const btnApprove50 = document.getElementById('btn-approve-50');
  const btnDecline = document.getElementById('btn-decline');
  const dodgeWrapper = document.getElementById('dodge-wrapper');
  
  const modal = document.getElementById('modal');
  const modalClose = document.getElementById('modal-close');
  const certRaise = document.getElementById('cert-raise');
  const certTitle = document.getElementById('cert-title');
  const btnCelebrateMore = document.getElementById('btn-celebrate-more');

  // Audio Synth Context
  let audioCtx;
  function playFanfare() {
    try {
      if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
      const now = audioCtx.currentTime;
      
      const notes = [261.63, 329.63, 392.00, 523.25, 659.25, 783.99]; // C E G C E G
      notes.forEach((freq, idx) => {
        const osc = audioCtx.createOscillator();
        const gain = audioCtx.createGain();
        osc.type = 'triangle';
        osc.frequency.setValueAtTime(freq, now + idx * 0.1);
        gain.gain.setValueAtTime(0.3, now + idx * 0.1);
        gain.gain.exponentialRampToValueAtTime(0.001, now + idx * 0.1 + 0.4);
        
        osc.connect(gain);
        gain.connect(audioCtx.destination);
        
        osc.start(now + idx * 0.1);
        osc.stop(now + idx * 0.1 + 0.4);
      });
    } catch (e) {
      console.log('Audio playback prevented or unsupported');
    }
  }

  // Update Score Logic
  function updateCalculations() {
    const tech = parseInt(sliderTech.value);
    const synergy = parseInt(sliderSynergy.value);
    const solve = parseInt(sliderSolve.value);

    valTech.textContent = tech + '%';
    valSynergy.textContent = synergy + '%';
    valSolve.textContent = solve + '%';

    const avg = Math.round((tech + synergy + solve + 100) / 3.4);
    overallScoreEl.textContent = avg + '%';

    // SVG circle offset calculation (full circle = 264)
    const pct = Math.min(avg / 200, 1);
    const offset = 264 - (264 * pct);
    scoreMeter.style.strokeDashoffset = offset;

    if (avg >= 150) {
      scoreVerdict.textContent = '🌟 PHENOMENAL EXECUTIVE MATERIAL';
      scoreVerdict.style.color = 'var(--accent-emerald)';
    } else if (avg >= 130) {
      scoreVerdict.textContent = '🔥 OUTSTANDING PERFORMANCE';
      scoreVerdict.style.color = 'var(--accent-cyan)';
    } else {
      scoreVerdict.textContent = '⚡ STRONG PROMOTION CANDIDATE';
      scoreVerdict.style.color = 'var(--accent-gold)';
    }
  }

  // Sliders Event Listeners
  [sliderTech, sliderSynergy, sliderSolve].forEach(s => {
    s.addEventListener('input', updateCalculations);
  });
  updateCalculations();

  // Dodging Decline Button
  let dodgeCount = 0;
  function dodgeDeclineButton(e) {
    dodgeCount++;
    if (dodgeCount === 1) {
      btnDecline.querySelector('#decline-text').textContent = '⚠️ Re-evaluating...';
    } else if (dodgeCount === 2) {
      btnDecline.querySelector('#decline-text').textContent = '🤔 Are you sure?';
    } else if (dodgeCount >= 3) {
      btnDecline.querySelector('#decline-text').textContent = '✨ Auto-Converted to 100% Bonus!';
      btnDecline.className = 'btn btn-gold';
      setTimeout(() => {
        triggerApproval('+100% Mega Bonus & Corner Office', 'Senior Executive VP');
      }, 400);
      return;
    }

    const wrapperRect = dodgeWrapper.getBoundingClientRect();
    const randomX = (Math.random() - 0.5) * 160;
    const randomY = (Math.random() - 0.5) * 60;
    btnDecline.style.transform = `translate(${randomX}px, ${randomY}px)`;
  }

  btnDecline.addEventListener('mouseenter', dodgeDeclineButton);
  btnDecline.addEventListener('click', (e) => {
    e.preventDefault();
    dodgeDeclineButton(e);
  });

  // Trigger Approval & Confetti
  function triggerApproval(raiseText, titleText) {
    certRaise.textContent = raiseText;
    certTitle.textContent = titleText;
    
    modal.classList.add('active');
    playFanfare();

    if (window.confetti) {
      confetti({
        particleCount: 120,
        spread: 80,
        origin: { y: 0.6 }
      });

      setTimeout(() => {
        confetti({
          particleCount: 60,
          angle: 60,
          spread: 55,
          origin: { x: 0 }
        });
        confetti({
          particleCount: 60,
          angle: 120,
          spread: 55,
          origin: { x: 1 }
        });
      }, 250);
    }
  }

  btnApprove30.addEventListener('click', () => {
    triggerApproval('+30% Base Salary Raise', 'Senior Lead Director of Innovation');
  });

  btnApprove50.addEventListener('click', () => {
    triggerApproval('+50% Base Salary Raise + Corner Office', 'Executive VP of Product & Engineering');
  });

  btnCelebrateMore.addEventListener('click', () => {
    playFanfare();
    if (window.confetti) {
      confetti({
        particleCount: 150,
        spread: 100,
        origin: { y: 0.5 }
      });
    }
  });

  modalClose.addEventListener('click', () => {
    modal.classList.remove('active');
  });

  modal.addEventListener('click', (e) => {
    if (e.target === modal) modal.classList.remove('active');
  });
});

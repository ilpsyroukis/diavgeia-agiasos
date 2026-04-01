document.addEventListener('DOMContentLoaded', () => {
    // Set default dates: from 1 month ago to today
    const dateEles = setDefaultDates();
    
    // UI Elements
    const searchBtn = document.getElementById('searchBtn');
    const searchInput = document.getElementById('searchInput');
    const loading = document.getElementById('loading');
    const errorCont = document.getElementById('error');
    const errorMsg = document.getElementById('errorMsg');
    const resultsMeta = document.getElementById('resultsMeta');
    const totalCount = document.getElementById('totalCount');
    const decisionsList = document.getElementById('decisionsList');

    // Handle search click
    searchBtn.addEventListener('click', () => {
        performSearch();
    });
    
    // Press Enter on input
    searchInput.addEventListener('keypress', (e) => {
        if(e.key === 'Enter') performSearch();
    });

    // Execute initial search on load!
    performSearch();

    function setDefaultDates() {
        const today = new Date();
        const past = new Date();
        past.setMonth(today.getMonth() - 1); // 1 month ago

        const startInput = document.getElementById('startDate');
        const endInput = document.getElementById('endDate');

        startInput.value = past.toISOString().split('T')[0];
        endInput.value = today.toISOString().split('T')[0];
        return { startInput, endInput };
    }

    async function performSearch() {
        const term = searchInput.value.trim();
        const start = document.getElementById('startDate').value;
        const end = document.getElementById('endDate').value;

        if(!start || !end) {
            showError('Παρακαλώ επιλέξτε ημερομηνίες.');
            return;
        }

        if(new Date(start) > new Date(end)) {
            showError('Η "Από" ημερομηνία πρέπει να είναι πριν την "Έως".');
            return;
        }

        // Hide old results, show loader
        hideError();
        resultsMeta.classList.add('hidden');
        decisionsList.innerHTML = '';
        loading.classList.remove('hidden');

        try {
            // Fetch local database
            const response = await fetch('./data.json');

            if(!response.ok) {
                throw new Error(`Σφάλμα επικοινωνίας τοπικής βάσης (HTTP ${response.status})`);
            }

            const allDecisions = await response.json();
            
            // Parse dates to timestamp for easy comparison
            const startTimeStamp = new Date(start).getTime();
            // End object needs +1 day to encompass the whole end day
            const endObj = new Date(end);
            endObj.setDate(endObj.getDate() + 1);
            const endTimeStamp = endObj.getTime();
            
            // Helper to strip Greek accents
            const stripAccents = (str) => str ? str.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase() : "";
            
            const cleanTerm = stripAccents(term);

            // Client-side filtering
            let filtered = allDecisions.filter(dec => {
                // 1. Check Date
                const dateMatches = dec.issueDate && dec.issueDate >= startTimeStamp && dec.issueDate < endTimeStamp;
                if (!dateMatches) return false;
                
                // 2. Check Term if provided
                if (cleanTerm) {
                    const subjectClean = stripAccents(dec.subject);
                    const adaClean = stripAccents(dec.ada);
                    if (!subjectClean.includes(cleanTerm) && !adaClean.includes(cleanTerm)) return false;
                }
                
                return true;
            });
            
            // Render maximum 150 items to keep DOM fast
            if (filtered.length > 150) {
                 filtered = filtered.slice(0, 150);
            }

            // Render
            renderDecisions(filtered);

        } catch (err) {
            console.error(err);
            showError('Αποτυχία άντλησης δεδομένων. Ελέγξτε τη σύνδεσή σας ή δοκιμάστε ξανά αργότερα.');
        } finally {
            loading.classList.add('hidden');
        }
    }

    function renderDecisions(decisions) {
        decisionsList.innerHTML = '';
        
        if (decisions.length === 0) {
            decisionsList.innerHTML = `<p style="grid-column: 1 / -1; text-align: center; color: var(--text-muted); font-weight: 500;">Δεν βρέθηκαν αποτελέσματα για αυτή την αναζήτηση.</p>`;
        } else {
            const html = decisions.map(createDecisionCard).join('');
            decisionsList.innerHTML = html;
        }

        totalCount.textContent = decisions.length;
        resultsMeta.classList.remove('hidden');
    }

    function createDecisionCard(dec) {
        // Format timestamp
        let dateStr = 'Άγνωστη Ημ/νία';
        if(dec.issueDate) {
            dateStr = new Date(dec.issueDate).toLocaleDateString('el-GR', {
                year: 'numeric', month: 'short', day: 'numeric'
            });
        }

        return `
            <article class="decision-card">
                <div>
                    <span class="decision-date">${dateStr}</span>
                    <h3 class="decision-subject" title="${dec.subject || ''}">
                        ${truncateText(dec.subject || 'Χωρίς Θέμα', 100)}
                    </h3>
                </div>
                <div class="decision-footer">
                    <span class="ada-badge">ΑΔΑ: ${dec.ada}</span>
                    <a href="https://diavgeia.gov.gr/doc/${dec.ada}" target="_blank" rel="noopener noreferrer" class="pdf-link">
                        Προβολή PDF <i class="ph ph-arrow-square-out"></i>
                    </a>
                </div>
            </article>
        `;
    }

    function truncateText(text, maxLength) {
        if(text.length <= maxLength) return text;
        return text.substring(0, maxLength) + '...';
    }

    function showError(msg) {
        errorMsg.textContent = msg;
        errorCont.classList.remove('hidden');
    }
    
    function hideError() {
        errorCont.classList.add('hidden');
    }
});

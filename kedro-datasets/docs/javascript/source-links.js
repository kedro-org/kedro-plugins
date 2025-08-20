// Make source code paths clickable and add [source] buttons
document.addEventListener('DOMContentLoaded', function() {
    // Function to create a [source] button
    function createSourceButton(githubUrl) {
        const sourceButton = document.createElement('a');
        sourceButton.href = githubUrl;
        sourceButton.target = '_blank';
        sourceButton.rel = 'noopener noreferrer';
        sourceButton.textContent = '[source]';
        sourceButton.style.cssText = `
            margin-left: 0.5em;
            font-size: 0.85em;
            color: #1976d2;
            text-decoration: none;
            border: 1px solid #1976d2;
            padding: 2px 6px;
            border-radius: 3px;
            background: transparent;
            font-family: monospace;
        `;
        sourceButton.addEventListener('mouseover', function() {
            this.style.backgroundColor = '#1976d2';
            this.style.color = 'white';
        });
        sourceButton.addEventListener('mouseout', function() {
            this.style.backgroundColor = 'transparent';
            this.style.color = '#1976d2';
        });
        return sourceButton;
    }

    // Function to extract file path from source path text
    function extractFilePath(text) {
        // Look for patterns like "Source code in kedro_datasets/..." or direct file paths
        const sourceMatch = text.match(/Source code in ([^\s]+\.py)/);
        if (sourceMatch) {
            return sourceMatch[1];
        }
        // Check if it's already a file path
        if (text.includes('kedro_datasets') && text.endsWith('.py')) {
            return text.trim();
        }
        return null;
    }

    // Add [source] buttons to class signatures
    function addSourceButtonsToSignatures() {
        console.log('Looking for signatures...');
        
        // Try multiple selectors to find the signature elements
        const possibleSelectors = [
            '.doc-signature',
            '.signature', 
            'h1',
            'h2', 
            'h3',
            'h4',
            'h5',
            'h6',
            '[data-md-component="content"] h1',
            '[data-md-component="content"] h2',
            '.md-typeset h1',
            '.md-typeset h2'
        ];
        
        let signatures = [];
        possibleSelectors.forEach(selector => {
            const elements = document.querySelectorAll(selector);
            signatures = signatures.concat(Array.from(elements));
        });
        
        console.log(`Found ${signatures.length} potential signature elements`);
        
        signatures.forEach(function(signature) {
            // Skip if already has a source button
            if (signature.querySelector('a[href*="github.com"]')) {
                return;
            }
            
            const text = signature.textContent || signature.innerText;
            console.log('Checking signature text:', text);
            
            // Look for class names that match our datasets - more flexible regex
            const classMatch = text.match(/(kedro_datasets(?:_experimental)?[\.\w]*)/);
            if (classMatch) {
                console.log('Found class match:', classMatch[1]);
                
                // Find the corresponding source path in the document
                const allElements = document.querySelectorAll('*');
                let sourceFilePath = null;
                
                for (let element of allElements) {
                    const elementText = element.textContent || element.innerText;
                    if (elementText.includes('Source code in') && elementText.includes('.py')) {
                        sourceFilePath = extractFilePath(elementText);
                        console.log('Found source file path:', sourceFilePath);
                        break;
                    }
                }
                
                if (sourceFilePath) {
                    const githubUrl = `https://github.com/kedro-org/kedro-plugins/blob/main/kedro-datasets/${sourceFilePath}`;
                    const sourceButton = createSourceButton(githubUrl);
                    
                    console.log('Adding source button to:', signature.tagName);
                    // Add the button to the signature
                    signature.appendChild(sourceButton);
                }
            }
        });
    }

    // Make existing source paths clickable
    function makeSourcePathsClickable() {
        console.log('Making source paths clickable...');
        const sourceElements = document.querySelectorAll('.doc-source-path, .doc-source, [class*="source"]');
        console.log(`Found ${sourceElements.length} source elements`);
        
        sourceElements.forEach(function(element) {
            const text = element.textContent || element.innerText;
            const filePath = extractFilePath(text);
            
            if (filePath) {
                const githubUrl = `https://github.com/kedro-org/kedro-plugins/blob/main/kedro-datasets/${filePath}`;
                
                // Create a clickable link
                const link = document.createElement('a');
                link.href = githubUrl;
                link.target = '_blank';
                link.rel = 'noopener noreferrer';
                link.textContent = text;
                link.style.textDecoration = 'none';
                link.style.color = 'inherit';
                
                // Replace the original text with the link
                element.innerHTML = '';
                element.appendChild(link);
            }
        });
    }

    // Handle mkdocstrings generated source code spans
    function handleSourceSpans() {
        console.log('Handling source spans...');
        const codeSpans = document.querySelectorAll('span, p, div');
        let foundSpans = 0;
        
        codeSpans.forEach(function(span) {
            const text = span.textContent || span.innerText;
            if (text.includes('kedro_datasets/') && text.includes('.py')) {
                foundSpans++;
                const filePath = text.trim();
                if (filePath.endsWith('.py')) {
                    console.log('Converting span to link:', filePath);
                    const githubUrl = `https://github.com/kedro-org/kedro-plugins/blob/main/kedro-datasets/${filePath}`;
                    
                    const link = document.createElement('a');
                    link.href = githubUrl;
                    link.target = '_blank';
                    link.rel = 'noopener noreferrer';
                    link.textContent = text;
                    link.style.color = '#1976d2';
                    link.style.textDecoration = 'underline';
                    
                    span.innerHTML = '';
                    span.appendChild(link);
                }
            }
        });
        console.log(`Found ${foundSpans} source spans`);
    }

    // Run all functions with multiple attempts
    function runAllFunctions() {
        console.log('Running source link functions...');
        addSourceButtonsToSignatures();
        makeSourcePathsClickable();
        handleSourceSpans();
    }

    // Try multiple times with different delays
    setTimeout(runAllFunctions, 100);
    setTimeout(runAllFunctions, 500);
    setTimeout(runAllFunctions, 1000);
    setTimeout(runAllFunctions, 2000);

    // Also run when content is dynamically loaded
    const observer = new MutationObserver(function(mutations) {
        mutations.forEach(function(mutation) {
            if (mutation.type === 'childList' && mutation.addedNodes.length > 0) {
                console.log('Content changed, re-running source link functions...');
                setTimeout(runAllFunctions, 100);
            }
        });
    });

    observer.observe(document.body, {
        childList: true,
        subtree: true
    });
});

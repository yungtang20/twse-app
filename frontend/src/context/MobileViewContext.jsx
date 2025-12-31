import React, { createContext, useState, useContext } from 'react';

const MobileViewContext = createContext();

export const MobileViewProvider = ({ children }) => {
    const [isMobileView, setIsMobileView] = useState(window.innerWidth < 768);

    React.useEffect(() => {
        const handleResize = () => {
            setIsMobileView(window.innerWidth < 768);
        };

        window.addEventListener('resize', handleResize);
        return () => window.removeEventListener('resize', handleResize);
    }, []);

    return (
        <MobileViewContext.Provider value={{ isMobileView, setIsMobileView }}>
            {children}
        </MobileViewContext.Provider>
    );
};

export const useMobileView = () => useContext(MobileViewContext);

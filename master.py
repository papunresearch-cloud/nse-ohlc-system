from parameter import update_all_parameters, calculate_single_script_parameters

    def handle_stock_addition(self, stock_name: str, ticker: str):
        """
        1. Seeds 300 historical rows.
        2. Initializes live index 0.
        3. Calculates technical parameters and commits to /param/<safe_key>.
        """
        safe_key = sanitize_key(stock_name)
        resolved_ticker = ticker or f"{safe_key}.NS"
        logger.info(f"[EVENT-HANDLER] ADD received for '{safe_key}' ({resolved_ticker}). Initiating instant sync...")

        self.replan_daily_routine()
        success, msg = sync_historical_script(safe_key, resolved_ticker, gap_trading_days=300, calendar=self.calendar)
        
        if success:
            logger.info(f"[EVENT-HANDLER] Historical 300 bars seeded for {safe_key}: {msg}")
            try:
                update_live_script(safe_key, resolved_ticker)
                logger.info(f"[EVENT-HANDLER] Live index 0 initialized for {safe_key}.")
            except Exception as e:
                logger.warning(f"[EVENT-HANDLER] Live index 0 init error for {safe_key}: {e}")

            # Calculate and populate technical parameters immediately
            try:
                logger.info(f"[EVENT-HANDLER] Triggering parameter calculation for {safe_key}...")
                param_success = calculate_single_script_parameters(safe_key)
                if param_success:
                    logger.info(f"[EVENT-HANDLER] /param/{safe_key} successfully populated! ✅")
            except Exception as p_err:
                logger.error(f"[EVENT-HANDLER] Parameter calculation failed for {safe_key}: {p_err}")
        else:
            logger.error(f"[EVENT-HANDLER] Historical sync failed for {safe_key}: {msg}")

    def handle_stock_deletion(self, stock_name: str):
        """
        Purges:
        1. /stocks/<safe_key> (OHLC History & Live Candle)
        2. /param/<safe_key> (Computed Technical Indicators)
        3. /display_list/stocks/<safe_key> (Monitor Settings)
        """
        safe_key = sanitize_key(stock_name)
        logger.info(f"[EVENT-HANDLER] DELETE received for '{safe_key}'. Commencing admin purge...")

        # 1. Purge OHLC database
        try:
            db.reference(f"stocks/{safe_key}").delete()
            logger.info(f"[EVENT-HANDLER] Successfully purged /stocks/{safe_key}")
        except Exception as e:
            logger.error(f"[EVENT-HANDLER] Failed to delete /stocks/{safe_key}: {e}")

        # 2. Purge technical parameter calculations
        try:
            db.reference(f"param/{safe_key}").delete()
            logger.info(f"[EVENT-HANDLER] Successfully purged /param/{safe_key}")
        except Exception as e:
            logger.error(f"[EVENT-HANDLER] Failed to delete /param/{safe_key}: {e}")

        # 3. Purge monitor display references
        try:
            db.reference(f"display_list/stocks/{safe_key}").delete()
            logger.info(f"[EVENT-HANDLER] Successfully purged /display_list/stocks/{safe_key}")
        except Exception as e:
            logger.error(f"[EVENT-HANDLER] Failed to delete display_list entry for {safe_key}: {e}")

        self.replan_daily_routine()